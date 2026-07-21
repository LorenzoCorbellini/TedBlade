const connect_to_db = require('./db');
const analyzeKeyPhrases = require('./localNLP');
const talk = require('./Talk');

// GET BY ID HANDLER

module.exports.get_by_id = async (event, context, callback) => {
    context.callbackWaitsForEmptyEventLoop = false;
    console.log('Received event:', JSON.stringify(event, null, 2));

    let body = {};
    if (event.body) {
        body = JSON.parse(event.body);
    }

    // Validazione: _id obbligatorio
    if (!body._id) {
        return callback(null, {
            statusCode: 400,
            headers: { 'Content-Type': 'text/plain' },
            body: 'Could not fetch the talk. _id is null.'
        });
    }

    try {
        await connect_to_db();
        console.log('=> get_by_id talk');

        // findById invece di find + limit
        const foundTalk = await talk.findOne({ _id: String(body._id) });

        if (!foundTalk) {
            return callback(null, {
                statusCode: 404,
                headers: { 'Content-Type': 'text/plain' },
                body: 'Talk not found.'
            });
        }

        // Analisi NLP se non ancora presente
        if (!foundTalk.comprehend_analysis && foundTalk.description) {
            try {
                const analysis = await analyzeKeyPhrases(foundTalk.description);
                foundTalk.comprehend_analysis = analysis;
                await foundTalk.save();
            } catch (err) {
                console.error(`Comprehend error on talk ${foundTalk._id}:`, err);
            }
        }

        // --- NUOVA LOGICA: Cerca i talk correlati tramite slug ---
        
        let watchNextSlugs = foundTalk.watch_next || [];
        
        // Se watch_next è una stringa singola anziché un array, lo convertiamo
        if (!Array.isArray(watchNextSlugs)) {
            watchNextSlugs = [watchNextSlugs];
        }

        // Cerca nel DB tutti i talk il cui 'slug' è presente nell'array 'watchNextSlugs'
        const watchNextTalks = await talk.find({ slug: { $in: watchNextSlugs } });

        // Ritorna i talk trovati completi (o puoi mapparli per ritornare solo i campi desiderati)
        return callback(null, {
            statusCode: 200,
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ 
                original_talk_id: foundTalk._id,
                watch_next_talks: watchNextTalks 
            })
        });

    } catch (err) {
        console.error('Error fetching talk:', err);
        return callback(null, {
            statusCode: err.statusCode || 500,
            headers: { 'Content-Type': 'text/plain' },
            body: 'Could not fetch the talk.'
        });
    }
};