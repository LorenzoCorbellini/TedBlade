/**
 * @description Questa lambda restituisce i talks presi da Mongodb Atlas con paginazione
 * @param {string} [event.queryStringParameters.page=1] - Il numero della pagina da recuperare (es. "1", "2").
 * @param {string} [event.queryStringParameters.limit=10] - Il numero di talk da restituire per pagina (es. "15").
 * 
 * @returns {Object} Risposta HTTP standard per API Gateway con status code e body JSON.
 * @returns {number} statusCode - 200 (Successo) o 500 (Errore del server).
 * @returns {string} body - Stringa JSON contenente:
 *   @property {Array<Object>} data - Array dei documenti dei talk recuperati dal database.
 *   @property {Object} meta - Metadati di paginazione fondamentali per Flutter.
 *     @property {number} meta.currentPage - La pagina attualmente restituita.
 *     @property {number} meta.limit - Il limite di elementi applicato.
 *     @property {number} meta.totalItems - Il numero totale di talk presenti nel database.
 *     @property {boolean} meta.hasMore - `true` se ci sono altri talk da caricare nelle pagine successive, altrimenti `false`.
 */

import { MongoClient } from 'mongodb'

const dbUsername = process.env.DB_USERNAME;
const dbPassword = process.env.DB_PASSWORD;
const url = `mongodb+srv://${dbUsername}:${dbPassword}@cluster0.hduxclv.mongodb.net/?appName=Cluster0"`

let cachedClient = null;

async function getClient() {
  if (!cachedClient) {
    cachedClient = await MongoClient.connect(url);
  }
  return cachedClient;
}

export const handler = async (event) => {
  try {
    const queryParams = event.queryStringParameters || {};
    const page = parseInt(queryParams.page) || 1;
    const limit = parseInt(queryParams.limit) || 10;

    const client = await getClient();
    const dbo = client.db("unibg_tedx_2026");
    const collection = dbo.collection("talks_full_data");

    const skipValue = (page - 1) * limit;

    const [result, totalDocuments] = await Promise.all([
      collection.find({})
        .sort({ _id: 1 })
        .skip(skipValue)
        .limit(limit)
        .toArray(),
      collection.countDocuments({}) // Numero di talks nel DB
    ]);

    const hasMore = skipValue + result.length < totalDocuments;

    return {
      statusCode: 200,
      headers: {
        "Content-Type": "application/json",
        "Access-Control-Allow-Origin": "*"
      },
      body: JSON.stringify({
        data: result,
        meta: {
          currentPage: page,
          limit: limit,
          totalItems: totalDocuments,
          hasMore: hasMore
          }
        }
      ),
    };
  } catch (err) {
    console.error("DB Error:", err);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: "Internal server error", details: err.message }),
    };
  }
};
