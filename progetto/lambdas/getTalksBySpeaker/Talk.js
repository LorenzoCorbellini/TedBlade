const mongoose = require('mongoose');

const talk_schema = new mongoose.Schema({
    speaker: String,
    talks: mongoose.Schema.Types.Mixed
}, { collection: 'speakers_full_data' });

module.exports = mongoose.model('talk', talk_schema);