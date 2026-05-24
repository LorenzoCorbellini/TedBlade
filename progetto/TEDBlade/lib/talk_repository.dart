import 'package:http/http.dart' as http;
import 'dart:convert';
import 'models/talk.dart';

Future<List<Talk>> initEmptyList() async {
  //create an empty list of talks
  Iterable list = json.decode("[]");
  var talks = list
      .map((model) => Talk.fromJSON(model))
      .toList(); //pass the empty list to the Talk.fromJSON method to create an empty list of talks
  return talks;
}

Future<List<Talk>> getTalksByTag(String tag, int page) async {
  var url = Uri.parse(
    'https://mg839u1xy1.execute-api.us-east-1.amazonaws.com/default/Get_Talks_By_Tag',
  );

  final http.Response response = await http.post(
    //make a POST request to the API with the tag, page, and doc_per_page parameters
    url,
    headers: <String, String>{
      'Content-Type': 'application/json',
    }, //pass the content type to the API
    body: jsonEncode(<String, Object>{
      //pass the tag, page, and doc_per_page parameters to the API
      'tag': tag,
      'page': page,
      'doc_per_page': 6,
    }),
  );
  if (response.statusCode == 200) {
    //if the response is successful, decode the response body and return a list of talks
    final body = utf8.decode(
      response.bodyBytes,
    ); //decode the response body to handle any special characters
    final List<dynamic> jsonList = json.decode(
      body,
    ); //decode the response body to a list of dynamic objects
    return jsonList
        .map((json) => Talk.fromJSON(json))
        .toList(); //map the list of dynamic objects to a list of talks using the Talk.fromJSON method
  } else {
    //if the response is not successful, throw an exception with the response status code and message
    throw Exception('Failed to load talks');
  }
}
