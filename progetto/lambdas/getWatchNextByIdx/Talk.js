const mongoose = require('mongoose');

const talk_schema = new mongoose.Schema({
    _id: String,
    title: String,
    url: String,
    description: String,
    speakers: String,
    watch_next: mongoose.Schema.Types.Mixed,  
    comprehend_analysis: mongoose.Schema.Types.Mixed
}, { collection: 'talks_full_data' });

module.exports = mongoose.model('talk', talk_schema);