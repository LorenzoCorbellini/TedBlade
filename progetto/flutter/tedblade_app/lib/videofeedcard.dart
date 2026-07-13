import 'package:flutter/material.dart';
import 'package:intl/intl.dart';
import 'package:tedblade_app/theme.dart';

class VideoFeedCard extends StatelessWidget {
  final String title;
  final String duration;
  final dynamic views;
  final String slug;
  final String thumbnailUrl;
  final String speakers;

  const VideoFeedCard({
    super.key,
    required this.title,
    required this.duration,
    required this.views,
    required this.slug,
    required this.thumbnailUrl,
    required this.speakers,
  });

  @override
  Widget build(BuildContext context) {
    return Container(
      decoration: BoxDecoration(
        color: AppTheme.colors.secondary,
        borderRadius: BorderRadius.circular(10),
      ),
      margin: const EdgeInsets.symmetric(vertical: 4),
      padding: const EdgeInsets.all(8),
      child: Row(
        children: [
          // Thumbnail
          Expanded(
            child: AspectRatio(
              aspectRatio: 16 / 9,
              child: Image.network(
                thumbnailUrl,
                fit: BoxFit.cover,
                errorBuilder: (context, error, stackTrace) {
                  return Container(
                    color: Colors.grey[800],
                    child: const Icon(
                      Icons.broken_image,
                      color: Colors.white24,
                    ),
                  );
                },
                loadingBuilder: (context, child, loadingProgress) {
                  if (loadingProgress == null) return child;
                  return Container(
                    color: Colors.grey[800],
                    child: const Center(
                      child: CircularProgressIndicator(strokeWidth: 2),
                    ),
                  );
                },
              ),
            ),
          ), // Fine thumbnail
          const SizedBox(width: 12),
          // Testo
          Expanded(
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              mainAxisSize: MainAxisSize.min,
              children: [
                Text(
                  title,
                  maxLines: 2,
                  overflow: TextOverflow.ellipsis,
                  style: AppTheme.text.semiBold.copyWith(fontSize: 20),
                ),
                const SizedBox(height: 6),
                Row(
                  children: [
                    const Icon(
                      Icons.person_outline,
                      color: Colors.black,
                      size: 18,
                    ),
                    const SizedBox(width: 4),
                    Expanded(
                      child: Text(
                        speakers,
                        maxLines: 1,
                        overflow: TextOverflow.ellipsis,
                        style: const TextStyle(
                          color: Colors.black,
                          fontSize: 18,
                        ),
                      ),
                    ),
                  ],
                ),
                const SizedBox(height: 4),
                Text(
                  '${formatViews(views['viewCount_ted'] ?? '0')} views',
                  style: AppTheme.text.regular.copyWith(fontSize: 14),
                ),
              ],
            ),
          ), // Fine testo
        ],
      ),
    );
  }

  String formatViews(dynamic views) {
    int viewCount = int.parse(views.toString());
    final compactFormatter = NumberFormat.compact(locale: 'en');
    return compactFormatter.format(viewCount);
  }
}
