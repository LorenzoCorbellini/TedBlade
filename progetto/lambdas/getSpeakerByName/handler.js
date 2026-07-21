const connect_to_db = require('./db');
const talk = require('./Talk');

// GET BY SPEAKER NAME HANDLER

module.exports.get_by_name = async (event, context, callback) => {
    context.callbackWaitsForEmptyEventLoop = false;
    console.log('Received event:', JSON.stringify(event, null, 2));

    let body = {};
    if (event.body) {
        body = JSON.parse(event.body);
    }

    // Validazione: name obbligatorio
    if (!body.name) {
        return callback(null, {
            statusCode: 400,
            headers: { 'Content-Type': 'text/plain' },
            body: 'Could not fetch the speaker. Name is null.'
        });
    }

    try {
        await connect_to_db();
        console.log('=> get_by_name speaker:', body.name);

        const foundSpeaker = await talk.findOne({ speaker: body.name });

        if (!foundSpeaker) {
            return callback(null, {
                statusCode: 404,
                headers: { 'Content-Type': 'text/plain' },
                body: 'Speaker not found.'
            });
        }

        return callback(null, {
            statusCode: 200,
            body: JSON.stringify(foundSpeaker)
        });

    } catch (err) {
        console.error('Error fetching speaker:', err);
        return callback(null, {
            statusCode: err.statusCode || 500,
            headers: { 'Content-Type': 'text/plain' },
            body: 'Could not fetch the speaker.'
        });
    }
};