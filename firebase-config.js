// Public Firebase + Cloudinary config for BRS chat.
// Values here ARE safe to commit: Firebase API keys are not secrets,
// and Cloudinary cloudName alone cannot upload (uploads are signed via Cloud Function).
//
// Fill these in AFTER creating the Firebase project and Cloudinary account
// following SETUP.md.

export const firebaseConfig = {
  apiKey: "AIzaSyCd9-8ItX9ZgaNkHOTth85WKCGrQXqlbK0",
  authDomain: "brs-chat-2026.firebaseapp.com",
  projectId: "brs-chat-2026",
  storageBucket: "brs-chat-2026.firebasestorage.app",
  messagingSenderId: "1081314197441",
  appId: "1:1081314197441:web:0127a9449bdfe5bbfff940",
};

export const cloudinaryConfig = {
  cloudName: "REPLACE_ME", // Phase A で設定
};

// Functions region must match the deploy region in functions/src/index.ts.
export const functionsRegion = "asia-northeast1";

// PHASE 0 = Spark plan verification mode (no Functions, no Cloudinary, no allowlist).
// Switch to false at Phase A after upgrading to Blaze and deploying Functions.
export const PHASE_0 = true;
