class Talk {
  final String title;
  final String details;
  final String mainSpeaker;
  final String url;
  final List<String> keyPhrases;

  Talk.fromJSON(Map<String, dynamic> jsonMap)
    : title =
          jsonMap['title'], //non può essere null, altrimenti si verifica un errore
      details =
          jsonMap['description'], //non può essere null, altrimenti si verifica un errore
      mainSpeaker =
          (jsonMap['speakers'] ??
          ""), //se il campo speakers è null, assegna una stringa vuota
      url =
          (jsonMap['url'] ??
          ""), //se il campo url è null, assegna una stringa vuota
      keyPhrases =
          (jsonMap['comprehend_analysis']['KeyPhrases'] as List<dynamic>?)
              ?.map((e) => e.toString())
              .toList() ??
          [];
}
