import 'package:flutter/material.dart';
import 'dart:convert';
import 'package:tedblade_app/fetch_utils.dart';
import 'package:tedblade_app/theme.dart';
import 'package:tedblade_app/videofeedcard.dart';
import 'package:http/http.dart' as http;

class HomePage extends StatefulWidget {
  const HomePage({super.key});

  @override
  State<HomePage> createState() => _HomePageState();
}

class _HomePageState extends State<HomePage> {
  List<dynamic> talksData = [];
  final controller = ScrollController();
  final client = http.Client();

  final int _limit = 10;
  int _page = 0;

  @override
  void initState() {
    super.initState();
    fetchNextTalksPage();
    controller.addListener(_scrollListener);
  }

  void _scrollListener() {
    if (controller.offset >= controller.position.maxScrollExtent - 100) {
      fetchNextTalksPage();
    }
  }

  void fetchNextTalksPage() {
    FetchUtils.fetchTalksPaginated(client, _page++, _limit)
        .then((response) {
          if (!mounted) return;
          final body = jsonDecode(response.body);
          final talks = body['data'];

          setState(() {
            talksData.addAll(talks);
          });
        })
        .catchError((error) {
          // TODO: display error
          print("Fetch error: $error");
        });
  }

  @override
  void dispose() {
    controller.dispose();
    client.close();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        title: Text(
          'TedBlade',
          style: AppTheme.text.bold.copyWith(fontSize: 32),
        ),
        backgroundColor: AppTheme.colors.secondary,
      ),

      body: Stack(
        children: [
          talksData.isEmpty
              ? const Center(child: CircularProgressIndicator())
              : Center(
                  child: ListView.builder(
                    controller: controller,
                    padding: const EdgeInsets.all(10),
                    itemCount: talksData.length + 1,
                    itemBuilder: (context, index) {
                      if (index < talksData.length) {
                        final talk = talksData[index];
                        return VideoFeedCard(
                          title: talk['title'],
                          duration: talk['duration'],
                          views: talk['statistics'],
                          slug: talk['slug'],
                          thumbnailUrl: talk['thumbnail_url'],
                          speakers: talk['speakers'],
                        );
                      } else {
                        return const Padding(
                          padding: EdgeInsets.symmetric(vertical: 32),
                          child: Center(child: CircularProgressIndicator()),
                        );
                      }
                    },
                  ),
                ),

          // Pulsante assistente AI
          Positioned(
            bottom: 16.0,
            left: 16.0,
            child: FloatingActionButton.small(
              onPressed: () {
                // TODO: implementare assistente AI
              },
              backgroundColor: AppTheme.colors.accent,
              elevation: 3,
              child: const Icon(
                Icons.auto_awesome_sharp,
                size: 18,
                color: Colors.white,
              ),
            ),
          ),
        ],
      ),

      // Pulsante con lente d'ingrandimento
      floatingActionButton: FloatingActionButton(
        onPressed: () {
          // TODO: implementare ricerca
        },
        backgroundColor: Colors.white,
        elevation: 3,
        shape: const CircleBorder(),
        child: const Icon(Icons.search, size: 36, color: Colors.black),
      ),

      floatingActionButtonLocation: FloatingActionButtonLocation.centerDocked,

      bottomNavigationBar: BottomNavigationBar(
        selectedLabelStyle: AppTheme.text.regular.copyWith(fontSize: 14),
        unselectedLabelStyle: AppTheme.text.regular.copyWith(fontSize: 12),
        items: const <BottomNavigationBarItem>[
          BottomNavigationBarItem(
            icon: Icon(Icons.chat_bubble),
            label: 'Talks',
          ),
          BottomNavigationBarItem(icon: Icon(Icons.person), label: 'Speakers'),
        ],
        // selectedItemColor: AppTheme.colors.accent,
        // currentIndex: _selectedIndex,
        // onTap: _onItemTapped,
      ),
    );
  }
}
