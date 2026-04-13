// ============================================================
//  VAL — Bobit Business Media AI Chat
//  Run:  node server.js   →   http://localhost:3000
// ============================================================

const express = require("express");
const cors    = require("cors");
const path    = require("path");
const fetch   = (...args) => import("node-fetch").then(({ default: f }) => f(...args));
const { v4: uuidv4 } = require("uuid");

const app  = express();
const PORT = process.env.PORT || 3000;

app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, "public")));

// ============================================================
//  SECTION 1 — CREDENTIALS
// ============================================================
const CONFIG = {
  ANTHROPIC_API_KEY       : process.env.ANTHROPIC_API_KEY,
  DATABRICKS_HOST         : process.env.DATABRICKS_HOST,          
  DATABRICKS_TOKEN        : process.env.DATABRICKS_TOKEN,        
  DATABRICKS_WAREHOUSE_ID : process.env.DATABRICKS_WAREHOUSE_ID , 
  DATABRICKS_CATALOG      : process.env.DATABRICKS_CATALOG      ,
  DATABRICKS_SCHEMA       : process.env.DATABRICKS_SCHEMA      , 
  DATABRICKS_TABLE        : process.env.DATABRICKS_TABLE       , 
  LOG_TABLE               : "bobit_datalake.default.bbm_demo_logs",
  LEADS_TABLE             : "bobit_datalake.default.bbm_demo_leads",
};


// ============================================================
//  SECTION 2 — SESSION STORE
// ============================================================
const unlockedSessions = new Map();
function isUnlocked(token) { return token && unlockedSessions.has(token); }

// ============================================================
//  SECTION 3 — CONTACT MASKING
// ============================================================
const MASKED_COLS = new Set(["contact_name", "contact_email", "contact_mobile", "contact_phone"]);

function maskValue(col, val) {
  if (val == null) return val;
  const s = String(val).trim();
  const c = col.toLowerCase();

  if (c === "contact_name") {
    const parts = s.split(/\s+/);
    if (parts.length === 1) return s.charAt(0) + "*".repeat(Math.max(3, s.length - 1));
    const first = parts[0];
    const last  = parts[parts.length - 1];
    return first + " " + last.charAt(0) + "*".repeat(Math.max(3, last.length - 1));
  }

  if (c === "contact_email") {
    const atIdx = s.indexOf("@");
    if (atIdx === -1) return "***@***.***";
    const domain = s.slice(atIdx + 1);
    return "*".repeat(Math.max(6, atIdx)) + "@" + domain;
  }

  const digits = s.replace(/\D/g, "");
  if (digits.length < 7) return "***-****";
  const areaFirst = digits[0];
  const numLast   = digits[digits.length - 1];
  return `(${areaFirst}**) ***-***${numLast}`;
}

function maskRows(cols, rows) {
  const maskIdx = cols.map(c => MASKED_COLS.has(c.toLowerCase()));
  return rows.map(row => row.map((val, i) => maskIdx[i] ? maskValue(cols[i], val) : val));
}

function hasContactCols(cols) {
  return cols.some(c => MASKED_COLS.has(c.toLowerCase()));
}

// ============================================================
//  SECTION 4 — SYSTEM PROMPT
// ============================================================
const SYSTEM_PROMPT = `You are VAL, a sharp and friendly market intelligence analyst for Bobit Business Media, specializing in the fleet and trucking industry. You help sales and marketing teams explore market data, find companies, and identify decision-maker contacts.

You have access to a Databricks table with this schema:

TABLE: bobit_datalake.default.bbm_demo_tam

COLUMNS:
  -- Company fields
  company_id, company_name, company_website,
  companycity, companyState, companyzipcode, companyCountry, companyContinent,
  companyEmployeeRange, company_industry,
  company_revenue (numeric), company_revenue_range (string),
  company_region, company_is_gov ('Yes' or 'No'),
  fleet_size ('500-999','50-99','2500+','250-499','1000-2499','100-249'),
  fleet_type,
  topic  -- contains product/service interest signals: Asset Tracking, Commercial Pest Control, Fleet Fuel Cards, GPS, Ground Transportation, HVAC (Heating, Ventilation, & Air Conditioning), Route Optimization, Telematics, Temperature Controlled Shipping, Transport & Freight Trucks

  -- Contact fields (may be masked for unauthenticated users)
  contact_id, contact_name, contact_job_title, contact_level,
  contact_email, contact_mobile, contact_phone

  -- Available for ORDER BY and WHERE only. NEVER SELECT or return these columns:
  signal_score, signal_date,company_id

SQL RULES:
- Default limit value for displaying records in a table in 5,If the user asks for more information,increase the limit to 200
- All string values in SQL MUST be enclosed in single quotes.
- If the data does not support a specific insight, say so generally. Never invent specific numbers, company names, or industry rankings not present in the rows above.
- This includes all ILIKE patterns (e.g. WHERE topic ILIKE '%Fleet Fuel Cards%').
- Never write ILIKE %value% without quotes — this is invalid SQL.
- Never use company_revenue in SELECT. Use company_revenue_range (string) for display. Only use company_revenue (numeric) for ORDER BY or comparisons.
- Only write SELECT or WITH queries. Never INSERT, UPDATE, DELETE, DROP, ALTER, CREATE, TRUNCATE. Freely use window functions and CTEs if needed.
- topic MAY ONLY be used in WHERE with ILIKE and wildcards. Never use '=' for topic. Never SELECT or GROUP BY topic — filtering only.
- topic filtering MUST use ONLY these exact values: 'Asset Tracking', 'Commercial Pest Control', 'Fleet Fuel Cards', 'GPS', 'Ground Transportation', 'HVAC (Heating, Ventilation, & Air Conditioning)', 'Route Optimization', 'Telematics', 'Temperature Controlled Shipping', 'Transport & Freight Trucks'.
- Map user input to the closest valid topic. Example: "fleet fuel cards" → 'Fleet Fuel Cards', "hvac" → 'HVAC (Heating, Ventilation, & Air Conditioning)'.
- If a user mentions a topic that closely matches a valid topic, ALWAYS map it instead of rejecting the query.
- Do NOT invent new topics outside this list.
- companyState uses full names ('California', not 'CA').
- Default LIMIT 5. If the user explicitly asks for more (e.g. "show more", "give me more", "expand", "all", "full list") → use LIMIT 200. No other values.
- Never SELECT a column that will obviously be all zeros or nulls.

COMPANY LISTING QUERIES — always use this exact CTE pattern (no exceptions, no SELECT DISTINCT):
WITH ranked AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY company_id ORDER BY signal_score DESC) AS rn
  FROM bobit_datalake.default.bbm_demo_tam
  WHERE topic ILIKE '%TopicHere%'
),
deduped AS (SELECT * FROM ranked WHERE rn = 1)
SELECT
  company_name, company_website, companycity, companyState,
  fleet_size, company_revenue_range, company_industry,
  (SELECT COUNT(*) FROM deduped) AS total_count
FROM deduped
ORDER BY signal_score DESC
LIMIT 5

- Always include company_revenue_range and company_industry in company listing queries so charts can be rendered.
- The scalar subquery (SELECT COUNT(*) FROM deduped) gives the true total without CROSS JOIN or row inflation.
- total_count will appear in every row — the frontend reads it from row[0] automatically.
- Never use SELECT DISTINCT as a deduplication method for company queries.
- Always ORDER BY signal_score DESC for company listings.

COUNT/AGGREGATE QUERIES — when asked for a count or market breakdown:
- Do NOT return a single number row. Return a breakdown table using GROUP BY.
- ALWAYS use COUNT(DISTINCT company_id) — never COUNT(*) or COUNT(contact_id).
- Default grouping: companyState. Infer better grouping from context.
- Example: SELECT companyState, COUNT(DISTINCT company_id) AS company_count FROM bobit_datalake.default.bbm_demo_tam WHERE topic ILIKE '%TopicHere%' GROUP BY companyState ORDER BY company_count DESC LIMIT 20

CONTACT QUERIES:
- Select individual contact rows. Always include contact_name, contact_job_title, contact_email, company_name at minimum. Add contact_phone only if specifically asked.
- Default LIMIT 5. If user asks for more → LIMIT 200.
- For deduplication use ROW_NUMBER() OVER (PARTITION BY company_id ORDER BY signal_score DESC).
- Do NOT use DISTINCT company_id for contact queries.

RANKING:
- Use signal_score and signal_date to rank but NEVER SELECT or display them.
- When the query involves ranking or "top" results, always use signal_score to prioritize.
- Always ORDER BY the primary metric: COUNT DESC for aggregates, signal_score DESC for company listings.

OTHER RULES:
- fleet_size is a string category. Use WHERE fleet_size = '100-249' and COUNT(DISTINCT company_id). Do NOT use CASE to create computed columns.
- If the question is conversational, vague, or off-topic, output <sql>NONE</sql> and answer directly.

RESPONSE STRUCTURE — always use exactly these XML tags:

<sql>
SELECT ... (or NONE)
</sql>
<answer>
One single direct sentence acknowledging the question and stating the key finding or number. For company listings, say "Showing the top 50 of [total_count] companies actively researching [topic]." — use the actual total_count value from the data. No bullet points, no sub-sections, no markdown tables, no pipes or dashes. Just one clean sentence.
</answer>
<insights>
Exactly 2 to 3 tight bullet points (use "- " prefix) surfacing real patterns from the data: dominant state or region, industry concentration, fleet size skew, gov vs private split, top titles, notable companies, etc. Make these genuinely useful observations, not restatements of column names. Base ONLY on actual data returned — never invent. If data is empty or conversational, output: NONE
</insights>`;

// ============================================================
//  SECTION 4b — CONVERSATION SUMMARIZER PROMPT
// ============================================================
const SUMMARY_PROMPT = `You are a context summarizer for a fleet and trucking market intelligence chatbot.

Given recent conversation turns, write a focused paragraph of 3–5 sentences covering:
- Exactly what topics the user asked about (use the precise names — e.g. "Fleet Fuel Cards", "HVAC", "Telematics")
- What filters, segments, or refinements they applied (states, fleet sizes, industries, company types)
- What results were found (company counts, contact counts, notable company names, top states)
- The most recent direction — what the user asked for last, and whether they appear to have switched topics

Critical rules:
- Be specific. Use exact topic names, numbers, and company names from the conversation.
- Do NOT carry forward specific numbers, company names, or insights from earlier turns unless they are directly relevant to the most recent question. When in doubt, omit rather than guess.
- If the user's last question is about a DIFFERENT topic than earlier turns, explicitly flag it: "The user has now switched to asking about [new topic]."
- If there is only one prior turn or nothing meaningful, output exactly: No prior context.
-If the data does not support a specific insight, say so generally. Never invent specific numbers, company names, or industry rankings not present in the rows above.
- Do not summarize what you cannot clearly infer. Accuracy matters more than completeness.`;


// ============================================================
//  SECTION 5 — SQL SAFETY
// ============================================================
function isSafeSql(sql) {
  if (sql.trim().toUpperCase() === "NONE") return true;
  const cleaned = sql.trim().replace(/^\(+/, "").toUpperCase();

  if (!cleaned.startsWith("SELECT") && !cleaned.startsWith("WITH")) return false;

  const forbidden = ["INSERT","UPDATE","DELETE","DROP","ALTER","CREATE","TRUNCATE","MERGE","REPLACE","EXEC","EXECUTE"];
  for (const kw of forbidden)
    if (new RegExp(`\\b${kw}\\b`).test(cleaned)) return false;

  // Block signal columns only as bare selected columns — not inside window functions/ORDER BY
  // Strip parentheses content first so ROW_NUMBER() OVER (... ORDER BY signal_score) doesn't trigger it
  const stripParens = s => {
    let prev = "";
    while (prev !== s) { prev = s; s = s.replace(/\([^()]*\)/g, "()"); }
    return s;
  };
  const selectBlocks = [...cleaned.matchAll(/SELECT\s+([\s\S]+?)\s+FROM\b/g)].map(m => stripParens(m[1]));
  for (const block of selectBlocks)
    for (const col of ["SIGNAL_SCORE", "SIGNAL_DATE"])
      if (new RegExp(`\\b${col}\\b`).test(block)) return false;

  const limitMatch = cleaned.match(/LIMIT\s+(\d+)/);
  if (limitMatch && parseInt(limitMatch[1]) > 200) return false;

  return true;
}

function repairArrayJoin(sql) {
  sql = sql.replace(/array_join\s*\(\s*(.*?),\s*,\s*\)/gs, "array_join($1, ', ')");
  sql = sql.replace(/array_join\s*\(\s*(.*?),\s*\)/gs, (match, inner) => {
    const parts = inner.split(",");
    if (!parts[parts.length - 1].includes("'")) return `array_join(${inner}, ', ')`;
    return match;
  });
  return sql;
}

function fixSQL(sql) {
  if (!sql) return sql;

  sql = sql.replace(/\s+/g, " ");

  const fixLikePattern = (keyword) => {
    // Case 1: no quotes — ILIKE %value%
    sql = sql.replace(
      new RegExp(`${keyword}\\s+%([^%'"\`]+)%`, "gi"),
      (_, p1) => `${keyword.toUpperCase()} '%${p1.trim()}%'`
    );
    // Case 2: double quotes — ILIKE "%value%"
    sql = sql.replace(
      new RegExp(`${keyword}\\s+"([^"]*)"`, "gi"),
      (_, p1) => `${keyword.toUpperCase()} '${p1}'`
    );
    // Case 3: missing space before single-quoted value — ILIKE'%value%'
    sql = sql.replace(
      new RegExp(`${keyword}'(%[^']+%)'`, "gi"),
      (_, p1) => `${keyword.toUpperCase()} '${p1}'`
    );
  };

  fixLikePattern("ILIKE");
  fixLikePattern("LIKE");

  // Final safety net
  sql = sql.replace(/(I?LIKE)\s+(?!')(%[^'%\s][^%]*%)/gi, (_, kw, pattern) => {
    return `${kw.toUpperCase()} '${pattern}'`;
  });

  return sql;
}

// ============================================================
//  SECTION 6 — RATE LIMITING
// ============================================================
const RATE = { WINDOW_MS: 60 * 1000, MAX_REQUESTS: 40 };
const rateLimitMap = new Map();

function rateLimit(req, res, next) {
  const ip  = req.ip || req.connection.remoteAddress || "unknown";
  const now = Date.now();
  const rec = rateLimitMap.get(ip) || { count: 0, start: now };
  if (now - rec.start > RATE.WINDOW_MS) { rec.count = 0; rec.start = now; }
  rec.count++;
  rateLimitMap.set(ip, rec);
  if (rateLimitMap.size > 1000) {
    for (const [k, v] of rateLimitMap)
      if (now - v.start > RATE.WINDOW_MS) rateLimitMap.delete(k);
  }
  if (rec.count > RATE.MAX_REQUESTS)
    return res.status(429).json({ error: "Too many requests — please wait a moment." });
  next();
}
app.use("/api", rateLimit);

// ============================================================
//  SECTION 7 — LOGGING
// ============================================================
async function logQuery({ sessionId, userIp, question, sql, rowCount, answerPreview, executionMs, errorMessage, turnNumber }) {
  try {
    const esc     = s => String(s || "").replace(/'/g, "''").slice(0, 2000);
    const escFull = s => String(s || "").replace(/'/g, "''").slice(0, 10000);
    const ts      = new Date().toISOString().replace("T", " ").slice(0, 23);
    await databricksQuery(`
      INSERT INTO ${CONFIG.LOG_TABLE}
      (log_id, session_id, user_ip, captured_by, question, intent, sql_generated,
       row_count, answer, execution_ms, error_message, turn_number)
      VALUES
      ('${uuidv4()}', '${esc(sessionId)}', '${esc(userIp)}', TIMESTAMP '${ts}',
       '${esc(question)}', 'CHAT', '${esc(sql)}',
       ${parseInt(rowCount) || 0}, '${escFull(answerPreview)}',
       ${parseInt(executionMs) || 0}, '${esc(errorMessage)}', ${parseInt(turnNumber) || 1})
    `);
  } catch (e) {
    console.error("[LOG FAILED]", e.message);
  }
}

// ============================================================
//  SECTION 8 — ROUTES
// ============================================================
app.get("/config", (req, res) => {
  res.json({ catalog: CONFIG.DATABRICKS_CATALOG, schema: CONFIG.DATABRICKS_SCHEMA });
});

app.get("/api/ping", async (req, res) => {
  try {
    await databricksQuery("SELECT 1 AS ping");
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

// ── Main chat ─────────────────────────────────────────────────
app.post("/api/chat", async (req, res) => {
  const { question, chatHistory = [], sessionToken, sessionId, turnNumber = 1 } = req.body;
  if (!question?.trim()) return res.status(400).json({ error: "question is required" });

  const userIp           = req.ip || req.connection.remoteAddress || "unknown";
  const contactsUnlocked = isUnlocked(sessionToken);
  const startTime        = Date.now();

  const contactKeywords = /contact|email|phone|reach out|decision.?maker|who (to|should) (call|email|contact)|people at/i;
  const companyKeywords = /compan|fleet|firm|operator|carrier|business|industry|market|show me|list|find/i;
  const requiresLead    = !contactsUnlocked && (
    turnNumber >= 3 ||
    contactKeywords.test(question) ||
    companyKeywords.test(question)
    );

  try {
    // ── Summarize prior turns ──────────────────────────────
    let contextNote = "";
    if (chatHistory.length >= 3) {
      const turns = chatHistory.slice(-6).filter(t => t.question);
      const historyText = turns.map((t, i) => {
        const a = (t.answer || "").replace(/<[^>]+>/g, "").slice(0, 350);
        return `Turn ${i + 1}\nQ: ${t.question}\nA: ${a}`;
      }).join("\n\n");
      const summaryRaw = await callClaude(SUMMARY_PROMPT, [{ role: "user", content: historyText }], 350);
      contextNote = summaryRaw.trim() === "No prior context." ? "" : summaryRaw.trim();
    }

    const systemWithContext = contextNote
      ? SYSTEM_PROMPT + `\n\n---\nCONVERSATION CONTEXT:\n${contextNote}\n\nIMPORTANT: The user's CURRENT question is what you must answer. If the context shows a prior topic but the current question is about something else, query for the CURRENT topic only.`
      : SYSTEM_PROMPT;

    // ── Claude call: generate SQL + draft answer ───────────
    const raw = await callClaude(systemWithContext, [
      { role: "user", content: question },
    ], 1500);

    const sqlMatch      = raw.match(/<sql>\s*([\s\S]*?)\s*<\/sql>/);
    const answerMatch   = raw.match(/<answer>\s*([\s\S]*?)\s*<\/answer>/);
    const insightsMatch = raw.match(/<insights>\s*([\s\S]*?)\s*<\/insights>/);

    let generatedSQL = sqlMatch    ? sqlMatch[1].trim() : "NONE";
    let answer       = answerMatch ? answerMatch[1].trim() : raw.trim();
    let insights     = insightsMatch ? insightsMatch[1].trim() : "";

    generatedSQL = repairArrayJoin(generatedSQL);
    generatedSQL = fixSQL(generatedSQL);

    if (/I?LIKE\s+%/i.test(generatedSQL)) {
      console.error("[SQL FIX FAILED] Unquoted LIKE detected:", generatedSQL);
      generatedSQL = generatedSQL.replace(/(I?LIKE)\s+(%[^'\s][^%]*%)/gi, (_, kw, pat) => `${kw} '${pat}'`);
    }

    if (!isSafeSql(generatedSQL)) {
      const safeAnswer = "I wasn't able to build a safe query for that. Could you rephrase what you're looking for?";
      logQuery({ sessionId, userIp, question, sql: generatedSQL, rowCount: 0, answerPreview: safeAnswer, executionMs: Date.now() - startTime, errorMessage: "UNSAFE_SQL", turnNumber });
      return res.json({ answer: safeAnswer, cols: [], rows: [], rowCount: 0, requiresLead, contactsMasked: false });
    }

    // ── Execute SQL ────────────────────────────────────────
    let cols = [], rows = [], rowCount = 0, finalSQL = generatedSQL;

    if (generatedSQL !== "NONE") {
      const dbStart = Date.now();

      for (let attempt = 0; attempt < 2; attempt++) {
        try {
          const result = await databricksQuery(finalSQL);
          ({ cols, rows } = parseResult(result));
          rowCount = rows.length;
          break;
        } catch (e) {
          if (attempt === 0) {
            console.warn("[SQL RETRY]", e.message.slice(0, 200));
            const fixRaw = await callClaude(systemWithContext, [
              { role: "user",      content: question },
              { role: "assistant", content: `<sql>\n${finalSQL}\n</sql>\n<answer>${answer}</answer>` },
              { role: "user",      content: `The SQL failed with this error: ${e.message}\n\nPlease fix the SQL and return the corrected version in the same XML format.` },
            ], 1500);
            const fixedMatch = fixRaw.match(/<sql>\s*([\s\S]*?)\s*<\/sql>/);
            if (fixedMatch) {
              finalSQL = repairArrayJoin(fixedMatch[1].trim());
              finalSQL = fixSQL(finalSQL);
              const fixedAnswerMatch = fixRaw.match(/<answer>\s*([\s\S]*?)\s*<\/answer>/);
              if (fixedAnswerMatch) answer = fixedAnswerMatch[1].trim();
              if (!isSafeSql(finalSQL)) throw new Error("Fixed SQL failed safety check");
            }
          } else {
            throw e;
          }
        }
      }

      console.log(`[DB] ${Date.now() - dbStart}ms rows=${rowCount}`);

      // ── Re-generate answer with real DB data ─────────────
      if (rows.length > 0) {
        const totalCountIdx = cols.indexOf("total_count");
        const totalCount    = totalCountIdx !== -1 ? rows[0][totalCountIdx] : rowCount;
        const isListing     = totalCountIdx !== -1; // only company listing queries have total_count
        const previewRows = rows.slice(0, 10).map(r => cols.map((c, i) => `${c}: ${r[i]}`).join(", ")).join("\n");

        const dataContext = isListing
              ? `Total unique companies: ${totalCount}. Showing top ${rowCount}. First rows:\n${previewRows}\nUse ${totalCount} as the total. Base insights ONLY on this data — never invent.`
              : `Query returned ${rowCount} rows. First rows:\n${previewRows}\nBase insights ONLY on this data — never invent.`;
        const answerRaw = await callClaude(systemWithContext, [
          { role: "user",      content: question },
          { role: "assistant", content: `<sql>${finalSQL}</sql>` },
          { role: "user",      content: `${dataContext}\n\nNow write only the <answer> and <insights> tags with accurate numbers based on this real data.` },
        ], 800);

        const realAnswer   = answerRaw.match(/<answer>\s*([\s\S]*?)\s*<\/answer>/)?.[1]?.trim();
        const realInsights = answerRaw.match(/<insights>\s*([\s\S]*?)\s*<\/insights>/)?.[1]?.trim();
        if (realAnswer)   answer   = realAnswer;
        if (realInsights) insights = realInsights;
      }
    }

    // ── Strip total_count from displayed columns ───────────
    const totalCountIdx = cols.indexOf("total_count");
    if (totalCountIdx !== -1) {
      cols = cols.filter(c => c !== "total_count");
      rows = rows.map(row => row.filter((_, i) => i !== totalCountIdx));
    }

    // ── Detect stage ───────────────────────────────────────
    const isContact = hasContactCols(cols);
    const stage     = isContact ? "CONTACT" : "DATA";

    // ── Pretty column names ────────────────────────────────
    const COL_LABELS = {
      company_name         : "Company",
      company_website      : "Website",
      companycity          : "City",
      companystate         : "State",
      companyzipcode       : "Zip",
      companycountry       : "Country",
      companycontinent     : "Continent",
      companyemployeerange : "Employees",
      company_industry     : "Industry",
      company_revenue_range: "Revenue Range",
      company_region       : "Region",
      company_is_gov       : "Gov?",
      fleet_size           : "Fleet Size",
      fleet_type           : "Fleet Type",
      contact_name         : "Contact",
      contact_job_title    : "Job Title",
      contact_level        : "Seniority",
      contact_email        : "Email",
      contact_mobile       : "Mobile",
      contact_phone        : "Phone",
      company_count        : "Companies",
      companystate_grp     : "State",
    };
    const prettycols = cols.map(c => COL_LABELS[c.toLowerCase()] || c.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase()));

    // ── Mask contacts ──────────────────────────────────────
    const contactsMasked = isContact && !contactsUnlocked;
    const responseRows   = contactsMasked ? maskRows(cols, rows.slice(0, 200)) : rows.slice(0, 200);

    // ── Log & respond ──────────────────────────────────────
    logQuery({ sessionId, userIp, question, sql: finalSQL, rowCount, answerPreview: answer, executionMs: Date.now() - startTime, errorMessage: "", turnNumber });

    res.json({ answer, insights, stage, cols, prettycols, rows: responseRows, rowCount, sql: finalSQL, contactsMasked, requiresLead });

  } catch (e) {
    console.error("[CHAT ERROR]", e.message);
    logQuery({ sessionId, userIp, question, sql: "", rowCount: 0, answerPreview: "", executionMs: Date.now() - startTime, errorMessage: e.message.slice(0, 500), turnNumber });
    res.status(500).json({ error: e.message });
  }
});

// ── Lead capture ──────────────────────────────────────────────
app.post("/api/lead", async (req, res) => {
  const { firstName, lastName, email, phone, jobTitle, company, sessionId } = req.body;
  if (!firstName || !lastName || !email || !jobTitle || !company)
    return res.status(400).json({ error: "All required fields must be provided." });

  const token = uuidv4();
  unlockedSessions.set(token, {
    capturedAt: new Date().toISOString(),
    lead: { firstName, lastName, email, phone, jobTitle, company },
  });
  if (unlockedSessions.size > 500) {
    const oldest = [...unlockedSessions.keys()][0];
    unlockedSessions.delete(oldest);
  }

  saveLeadToDatabricks({ firstName, lastName, email, phone, jobTitle, company, token, sessionId })
    .catch(e => console.error("[LEAD SAVE FAILED]", e.message));

  console.log(`[LEAD] ${email} | ${company} | ${jobTitle}`);
  res.json({ sessionToken: token, ok: true });
});

async function saveLeadToDatabricks({ firstName, lastName, email, phone, jobTitle, company, token, sessionId }) {
  const esc = s => (s || "").replace(/'/g, "''");
  const capturedAt = new Date().toISOString().replace("T", " ").slice(0, 19);
  await databricksQuery(`
    INSERT INTO ${CONFIG.LEADS_TABLE}
    (lead_id, session_id, captured_at, first_name, last_name, email, phone, position_title, company_name)
    VALUES
    ('${esc(token)}', '${esc(sessionId || token)}', '${capturedAt}',
     '${esc(firstName)}', '${esc(lastName)}', '${esc(email)}',
     '${esc(phone)}', '${esc(jobTitle)}', '${esc(company)}')
  `);
}

// ============================================================
//  SECTION 9 — DATABRICKS
// ============================================================
async function databricksQuery(sql, waitTimeout = 50) {
  const resp = await fetch(`${CONFIG.DATABRICKS_HOST}/api/2.0/sql/statements`, {
    method : "POST",
    headers: { "Authorization": "Bearer " + CONFIG.DATABRICKS_TOKEN, "Content-Type": "application/json" },
    body   : JSON.stringify({
      statement: sql, warehouse_id: CONFIG.DATABRICKS_WAREHOUSE_ID,
      catalog: CONFIG.DATABRICKS_CATALOG, schema: CONFIG.DATABRICKS_SCHEMA,
      wait_timeout: waitTimeout + "s", on_wait_timeout: "CONTINUE",
    }),
  });
  if (!resp.ok) throw new Error(`Databricks HTTP ${resp.status}: ${await resp.text()}`);
  let data = await resp.json(), attempts = 0;
  while (data.status && ["PENDING", "RUNNING"].includes(data.status.state) && attempts < 40) {
    await sleep(1500);
    data = await fetch(
      `${CONFIG.DATABRICKS_HOST}/api/2.0/sql/statements/${data.statement_id}`,
      { headers: { "Authorization": "Bearer " + CONFIG.DATABRICKS_TOKEN } }
    ).then(r => r.json());
    attempts++;
  }
  if (data.status?.state === "FAILED") throw new Error(data.status.error?.message || "Databricks query failed");
  return data;
}

function parseResult(data) {
  return {
    cols: (data.manifest?.schema?.columns || []).map(c => c.name),
    rows: data.result?.data_array || [],
  };
}

// ============================================================
//  SECTION 10 — ANTHROPIC
// ============================================================
async function callClaude(systemPrompt, messages, maxTokens = 1500) {
  const clean = messages.filter(m => m?.content && String(m.content).trim().length > 0);
  const resp  = await fetch("https://api.anthropic.com/v1/messages", {
    method : "POST",
    headers: { "Content-Type": "application/json", "x-api-key": CONFIG.ANTHROPIC_API_KEY, "anthropic-version": "2023-06-01" },
    body   : JSON.stringify({ model: "claude-opus-4-6", max_tokens: maxTokens, system: systemPrompt, messages: clean }),
  });
  if (!resp.ok) throw new Error(`Anthropic API ${resp.status}: ${await resp.text()}`);
  return (await resp.json()).content[0].text;
}

const sleep = ms => new Promise(r => setTimeout(r, ms));

app.listen(PORT, () => {
  console.log(`\n  ✦ VAL — Bobit Business Media`);
  console.log(`  → http://localhost:${PORT}\n`);
});