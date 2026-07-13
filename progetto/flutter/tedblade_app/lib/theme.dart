import 'package:flutter/material.dart';
import 'package:google_fonts/google_fonts.dart';

class AppTheme {
  AppTheme._();

  static final colors = _AppColors();
  static final text = _AppTextStyles();
}

class _AppColors {
  const _AppColors();
  
  final primary = const Color(0xff2f27ce);
  final secondary = const Color(0xffDEDCFF);
  final accent = const Color(0xff433bff);
  final background = const Color(0xfffbfbfe);

  final textBlack = const Color(0xff050315);
}

class _AppTextStyles {
  _AppTextStyles();
  // W100 - Extra light
  final extraLight = GoogleFonts.inter(fontWeight: FontWeight.w100);
  
  // W300 - Light
  final light = GoogleFonts.inter(fontWeight: FontWeight.w300);
  
  // W400 - Regular
  final regular = GoogleFonts.inter(fontWeight: FontWeight.w400);
  
  // W500 - Medium
  final medium = GoogleFonts.inter(fontWeight: FontWeight.w500);
  
  // W600 - Semi-Bold
  final semiBold = GoogleFonts.inter(fontWeight: FontWeight.w600);
  
  // W700 - Bold
  final bold = GoogleFonts.inter(fontWeight: FontWeight.w700);
  
  // W900 - Extra bold
  final extraBold = GoogleFonts.inter(fontWeight: FontWeight.w900);
}