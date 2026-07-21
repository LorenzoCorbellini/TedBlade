/* Mappiamo gli oggetto mongodb ad oggetti nodejs */

const mongoose = require('mongoose');

const talk_schema = new mongoose.Schema({
    title: String,
    url: String,
    description: String,
    speakers: String,
    comprehend_analysis: mongoose.Schema.Types.Mixed
}, { collection: 'tedx_data' });

/* 
    Mongoose permette di mappare una collection mongodb su un oggetto nodejs
    Ogni volta che usiamo l'oggetto talk stiamo accedendo alla collezione
*/
module.exports = mongoose.model('talk', talk_schema);