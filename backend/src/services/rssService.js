import Parser from "rss-parser";
import sqlite3 from "sqlite3";
import { open } from "sqlite";
import fs from "fs";
import path from "path";
import { rssSources } from "../../config/rssSources.js";
import geminiService from "./geminiService.js";

const parser = new Parser({
  customFields: {
    item: [
      ['media:content', 'mediaContent'],
      ['media:thumbnail', 'mediaThumbnail'],
      ['content:encoded', 'contentEncoded']
    ]
  }
});

let db;
const dataDir = path.resolve("./data");
const dbPath = path.join(dataDir, "rss_cache.db");

// Initialize SQLite Database
export async function initDB() {
  try {
    if (!fs.existsSync(dataDir)) {
      fs.mkdirSync(dataDir, { recursive: true });
    }

    db = await open({
      filename: dbPath,
      driver: sqlite3.Database
    });

    await db.exec(`
      CREATE TABLE IF NOT EXISTS articles (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT NOT NULL,
        link TEXT UNIQUE NOT NULL,
        content TEXT,
        summary TEXT,
        pubDate TEXT,
        source TEXT,
        category TEXT,
        imageUrl TEXT,
        author TEXT,
        domains TEXT,
        readMinutes INTEGER DEFAULT 3,
        credibilityScore INTEGER,
        validationStatus TEXT DEFAULT 'pending',
        isGenerated BOOLEAN DEFAULT 0,
        createdAt DATETIME DEFAULT CURRENT_TIMESTAMP,
        updatedAt DATETIME DEFAULT CURRENT_TIMESTAMP
      )
    `);

    await db.exec(`
      CREATE INDEX IF NOT EXISTS idx_articles_pubDate ON articles(pubDate DESC);
      CREATE INDEX IF NOT EXISTS idx_articles_category ON articles(category);
      CREATE INDEX IF NOT EXISTS idx_articles_source ON articles(source);
      CREATE INDEX IF NOT EXISTS idx_articles_createdAt ON articles(createdAt DESC);
    `);

    console.log("✅ RSS SQLite database initialized");
    return db;
  } catch (error) {
    console.error("❌ Failed to initialize RSS database:", error);
    throw error;
  }
}

// Domain classification using keywords
function classifyDomains(title = '', content = '', category = '') {
  const text = `${title} ${content} ${category}`.toLowerCase();
  const domains = new Set();

  if (/\b(war|sanction|diplomacy|election|border|conflict|geopolitic)/.test(text)) {
    domains.add('geopolitics');
  }
  if (/\b(trade|tariff|export|import|supply chain|logistics)\b/.test(text)) {
    domains.add('trade');
  }
  if (/\b(sport|league|tournament|match|world cup|olympic)/.test(text)) {
    domains.add('sports');
  }
  if (/\b(gdp|inflation|recession|macro|economy|economic)\b/.test(text)) {
    domains.add('economics');
  }
  if (/\b(index|s&p|nasdaq|dow|ftse|nifty|sensex|stock market|equity|shares?)\b/.test(text)) {
    domains.add('share market');
  }
  if (/\b(bank|interest rate|loan|funding|capital|investment|investor|finance)\b/.test(text)) {
    domains.add('finance');
  }
  if (/\b(marketing|brand|campaign|advertis(ing|ement)|customer|consumer)\b/.test(text)) {
    domains.add('marketing');
  }
  if (/\b(ai |artificial intelligence|machine learning|cloud|software|startup|tech\b)/.test(text)) {
    domains.add('technology');
  }

  if (domains.size === 0) {
    domains.add(category || 'general');
  }

  return Array.from(domains);
}

// Extract image URL from RSS item
function extractImageUrl(item) {
  // Try different image fields
  if (item.enclosure && item.enclosure.url) {
    return item.enclosure.url;
  }
  if (item.mediaContent && item.mediaContent.$?.url) {
    return item.mediaContent.$.url;
  }
  if (item.mediaThumbnail && item.mediaThumbnail.$?.url) {
    return item.mediaThumbnail.$.url;
  }
  if (item['media:content'] && item['media:content'].$?.url) {
    return item['media:content'].$.url;
  }
  
  // Try to extract from content
  const content = item.content || item.contentEncoded || item.description || '';
  const imgMatch = content.match(/<img[^>]+src="([^">]+)"/);
  if (imgMatch) {
    return imgMatch[1];
  }

  return null;
}

// Calculate read time
function calculateReadMinutes(content) {
  if (!content) return 3;
  const words = content.split(/\s+/).length;
  return Math.max(2, Math.ceil(words / 200)); // 200 words per minute
}

// Fetch RSS Feeds
export async function fetchRSSFeeds() {
  console.log("🔄 Fetching RSS feeds...");
  
  if (!db) {
    await initDB();
  }

  let totalFetched = 0;
  let totalSaved = 0;
  const errors = [];

  for (const source of rssSources) {
    try {
      console.log(`  📡 Fetching: ${source.name}`);
      const feed = await parser.parseURL(source.url);

      for (const item of feed.items) {
        try {
          const content = item.contentSnippet || item.content || item.description || '';
          const summary = content.substring(0, 300).trim();
          const domains = classifyDomains(item.title, content, source.category);
          const imageUrl = extractImageUrl(item);
          const readMinutes = calculateReadMinutes(content);

          await db.run(
            `
            INSERT OR IGNORE INTO articles
            (title, link, content, summary, pubDate, source, category, imageUrl, author, domains, readMinutes)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            `,
            [
              item.title,
              item.link,
              content,
              summary,
              item.pubDate || new Date().toISOString(),
              source.name,
              source.category,
              imageUrl,
              item.creator || item.author || null,
              JSON.stringify(domains),
              readMinutes
            ]
          );

          totalFetched++;
          totalSaved++;
        } catch (itemError) {
          // Skip duplicate or invalid items
          if (!itemError.message.includes('UNIQUE constraint')) {
            console.error(`    ⚠️ Item error: ${itemError.message}`);
          }
        }
      }

      console.log(`    ✅ ${source.name}: ${feed.items.length} items processed`);
    } catch (err) {
      console.error(`    ❌ ${source.name}: ${err.message}`);
      errors.push({ source: source.name, error: err.message });
    }
  }

  console.log(`✅ RSS update complete: ${totalFetched} fetched, ${totalSaved} saved`);
  
  return {
    totalFetched,
    totalSaved,
    errors,
    timestamp: new Date().toISOString()
  };
}

// Get articles from cache
export async function getArticles(options = {}) {
  if (!db) {
    await initDB();
  }

  const {
    category = null,
    limit = 30,
    offset = 0,
    domains = null,
    search = null
  } = options;

  let query = "SELECT * FROM articles WHERE 1=1";
  const params = [];

  if (category) {
    query += " AND category = ?";
    params.push(category);
  }

  if (domains) {
    query += " AND domains LIKE ?";
    params.push(`%${domains}%`);
  }

  if (search) {
    query += " AND (title LIKE ? OR content LIKE ?)";
    params.push(`%${search}%`, `%${search}%`);
  }

  query += " ORDER BY pubDate DESC LIMIT ? OFFSET ?";
  params.push(limit, offset);

  const articles = await db.all(query, params);

  // Parse domains JSON and normalize for frontend (url + sourceName)
  return articles.map(article => ({
    ...article,
    url: article.link || article.url,
    sourceName: article.sourceName || article.source,
    publishedAt: article.publishedAt || article.pubDate,
    domains: JSON.parse(article.domains || '[]'),
    isGenerated: article.isGenerated === 1
  }));
}

// Get article by ID
export async function getArticleById(id) {
  if (!db) {
    await initDB();
  }

  const article = await db.get("SELECT * FROM articles WHERE id = ?", [id]);
  
  if (article) {
    article.domains = JSON.parse(article.domains || '[]');
    article.isGenerated = article.isGenerated === 1;
    article.url = article.link || article.url;
    article.sourceName = article.sourceName || article.source;
    article.publishedAt = article.publishedAt || article.pubDate;
  }

  return article;
}

// Get statistics
export async function getStats() {
  if (!db) {
    await initDB();
  }

  const stats = await db.get(`
    SELECT 
      COUNT(*) as total,
      COUNT(DISTINCT source) as sources,
      COUNT(DISTINCT category) as categories,
      AVG(credibilityScore) as avgCredibility
    FROM articles
  `);

  const categoryDist = await db.all(`
    SELECT category, COUNT(*) as count
    FROM articles
    GROUP BY category
    ORDER BY count DESC
  `);

  const sourceDist = await db.all(`
    SELECT source, COUNT(*) as count
    FROM articles
    GROUP BY source
    ORDER BY count DESC
    LIMIT 10
  `);

  return {
    ...stats,
    categoryDistribution: categoryDist.reduce((acc, row) => {
      acc[row.category] = row.count;
      return acc;
    }, {}),
    sourceDistribution: sourceDist.reduce((acc, row) => {
      acc[row.source] = row.count;
      return acc;
    }, {})
  };
}

// Clean old articles (older than 7 days)
export async function cleanupOldArticles(daysOld = 7) {
  if (!db) {
    await initDB();
  }

  const result = await db.run(
    `DELETE FROM articles WHERE createdAt < datetime('now', '-${daysOld} days')`
  );

  console.log(`🧹 Cleaned up ${result.changes} old articles (older than ${daysOld} days)`);
  return result.changes;
}

// Enhance articles with Gemini AI
export async function enhanceArticlesWithAI(limit = 10) {
  if (!db || !geminiService.isConfigured) {
    console.log("⚠️ AI enhancement skipped: Gemini not configured");
    return 0;
  }

  const articles = await db.all(
    `SELECT * FROM articles 
     WHERE validationStatus = 'pending' 
     ORDER BY createdAt DESC 
     LIMIT ?`,
    [limit]
  );

  let enhanced = 0;

  for (const article of articles) {
    try {
      const validation = await geminiService.validateNewsArticle({
        title: article.title,
        sourceName: article.source,
        summary: article.summary,
        url: article.link
      });

      await db.run(
        `UPDATE articles 
         SET credibilityScore = ?, validationStatus = ?, updatedAt = CURRENT_TIMESTAMP
         WHERE id = ?`,
        [validation.credibilityScore, validation.recommendation, article.id]
      );

      enhanced++;
    } catch (error) {
      console.error(`AI enhancement failed for article ${article.id}:`, error.message);
    }
  }

  console.log(`🤖 Enhanced ${enhanced} articles with AI`);
  return enhanced;
}

export default {
  initDB,
  fetchRSSFeeds,
  getArticles,
  getArticleById,
  getStats,
  cleanupOldArticles,
  enhanceArticlesWithAI
};
