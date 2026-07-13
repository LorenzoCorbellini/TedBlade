import 'package:http/http.dart' as http;

class FetchUtils {
  static Future<http.Response> fetchTalksPaginated(http.Client client, int page, int limit) async {
    final url = Uri.parse(
        'https://rm11pc85ha.execute-api.us-east-1.amazonaws.com/getTalksPaginated?page=$page&limit=$limit'
    );
    final response = await client.get(url);
    return response;
  }

  static Future<http.Response> fetchSpeakersPaginated(http.Client client, int page, int limit) async {
    final url = Uri.parse(
        'https://rm11pc85ha.execute-api.us-east-1.amazonaws.com/getSpeakersPaginated?page=$page&limit=$limit'
    );
    final response = await client.get(url);
    return response;
  }
}