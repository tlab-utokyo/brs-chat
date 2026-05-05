// FCM service worker for background push notifications.
// Loaded by the browser via navigator.serviceWorker.register('./firebase-messaging-sw.js').
//
// Service workers can't use ES modules in all browsers, so we rely on the
// compat builds from gstatic. Keep the version in sync with chat.js.

importScripts('https://www.gstatic.com/firebasejs/10.13.2/firebase-app-compat.js');
importScripts('https://www.gstatic.com/firebasejs/10.13.2/firebase-messaging-compat.js');

// Inlined config (the SW can't import firebase-config.js ES module). If you
// rotate any of these values in firebase-config.js, mirror them here.
firebase.initializeApp({
  apiKey: "AIzaSyCd9-8ItX9ZgaNkHOTth85WKCGrQXqlbK0",
  authDomain: "brs-chat-2026.firebaseapp.com",
  projectId: "brs-chat-2026",
  storageBucket: "brs-chat-2026.firebasestorage.app",
  messagingSenderId: "1081314197441",
  appId: "1:1081314197441:web:0127a9449bdfe5bbfff940",
});

const messaging = firebase.messaging();

// Background handler — fires when the tab is closed/hidden. Foreground
// notifications are handled by chat.js via onMessage(). Foreground messages
// hitting the SW (because the tab is not visible) are delivered here as well.
messaging.onBackgroundMessage((payload) => {
  const data = payload.data || {};
  const title = data.title || 'BRS Community';
  const body = data.body || '';
  const channelId = data.channelId;
  const notifTag = data.notifTag || channelId || 'brs-chat';

  self.registration.showNotification(title, {
    body: body.slice(0, 180),
    icon: data.icon || './logo.png',
    badge: './logo.png',
    tag: notifTag,
    renotify: false,
    data: { channelId, url: data.url },
  });
});

// Click → focus existing tab or open a new one on the right channel.
self.addEventListener('notificationclick', (event) => {
  event.notification.close();
  const data = event.notification.data || {};
  const targetUrl = data.url || './';
  event.waitUntil((async () => {
    const clientsList = await self.clients.matchAll({
      type: 'window',
      includeUncontrolled: true,
    });
    for (const client of clientsList) {
      // Reuse any existing chat tab — postMessage so chat.js can switch channel
      // without a full reload.
      if (client.url.includes('/chat') || client.url.includes('/brs-chat')) {
        client.postMessage({ type: 'fcm-notification-click', channelId: data.channelId });
        return client.focus();
      }
    }
    return self.clients.openWindow(targetUrl);
  })());
});
