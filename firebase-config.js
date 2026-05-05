// Public Firebase + Cloudinary config for BRS chat.
// Values here ARE safe to commit: Firebase API keys are not secrets,
// and Cloudinary cloudName alone cannot upload (uploads are signed via Cloud Function).

export const firebaseConfig = {
  apiKey: "AIzaSyCd9-8ItX9ZgaNkHOTth85WKCGrQXqlbK0",
  authDomain: "brs-chat-2026.firebaseapp.com",
  projectId: "brs-chat-2026",
  storageBucket: "brs-chat-2026.firebasestorage.app",
  messagingSenderId: "1081314197441",
  appId: "1:1081314197441:web:0127a9449bdfe5bbfff940",
};

export const cloudinaryConfig = {
  cloudName: "dhhp1jdqd",
};

// Functions region must match the deploy region in functions/src/index.ts.
export const functionsRegion = "asia-northeast1";

// PHASE 0 = Spark plan mode (no Functions, no claim check). Phase A flips this
// to false: the brsMember custom claim is then required for any Firestore op.
export const PHASE_0 = false;

// Toggles the image-attach button. Stays false until the getUploadSignature
// callable + Cloudinary upload flow is wired into chat.js. Flip to true once
// the image upload UI integration is complete.
export const IMAGE_UPLOAD_ENABLED = false;
