// BRS Community Chat — main client script.
// Depends on Firebase v10 modular SDK (loaded from gstatic CDN).
// Cloudinary uploads use signed params from a Cloud Function.

import { initializeApp } from "https://www.gstatic.com/firebasejs/10.13.2/firebase-app.js";
import {
  getAuth, GoogleAuthProvider, signInWithPopup, signOut,
  onAuthStateChanged, sendSignInLinkToEmail, isSignInWithEmailLink,
  signInWithEmailLink,
} from "https://www.gstatic.com/firebasejs/10.13.2/firebase-auth.js";
import {
  getFirestore, collection, doc, addDoc, setDoc, updateDoc, deleteDoc,
  getDoc, getDocs, query, where, orderBy, limit, onSnapshot, runTransaction,
  serverTimestamp, Timestamp, arrayUnion, arrayRemove, increment, deleteField,
} from "https://www.gstatic.com/firebasejs/10.13.2/firebase-firestore.js";
import {
  getFunctions, httpsCallable,
} from "https://www.gstatic.com/firebasejs/10.13.2/firebase-functions.js";
import {
  getMessaging, getToken, onMessage, deleteToken,
  isSupported as isMessagingSupported,
} from "https://www.gstatic.com/firebasejs/10.13.2/firebase-messaging.js";

import { firebaseConfig, cloudinaryConfig, functionsRegion, PHASE_0, IMAGE_UPLOAD_ENABLED, FCM_VAPID_KEY } from "./firebase-config.js";

// ===========================================================================
// Setup
// ===========================================================================

const app = initializeApp(firebaseConfig);
const auth = getAuth(app);
const db = getFirestore(app);
const functions = getFunctions(app, functionsRegion);

const googleProvider = new GoogleAuthProvider();
googleProvider.setCustomParameters({ prompt: "select_account" });

// Hide image attach UI until the chat.js / getUploadSignature integration
// is finished.
if (!IMAGE_UPLOAD_ENABLED) {
  document.addEventListener("DOMContentLoaded", () => {
    const attachBtn = document.getElementById("btn-attach");
    if (attachBtn) attachBtn.style.display = "none";
  });
}

// Generic handler for all × close buttons marked with data-close-dialog.
document.addEventListener("click", (e) => {
  const t = e.target.closest("[data-close-dialog]");
  if (!t) return;
  const id = t.getAttribute("data-close-dialog");
  const dlg = document.getElementById(id);
  if (dlg?.open) dlg.close();
});

// Inherit theme from parent site if embedded later.
(function initTheme() {
  try {
    const parentTheme = window.parent?.document?.documentElement?.dataset?.theme;
    if (parentTheme) document.documentElement.dataset.theme = parentTheme;
  } catch (_) { /* cross-origin, ignore */ }
  if (!document.documentElement.dataset.theme) {
    const prefersDark = window.matchMedia("(prefers-color-scheme: dark)").matches;
    document.documentElement.dataset.theme = prefersDark ? "dark" : "light";
  }
})();

// ===========================================================================
// DOM refs
// ===========================================================================

const $ = (id) => document.getElementById(id);
const el = {
  inviteScreen: $("invite-screen"),
  formInvite: $("form-invite"),
  inputInvite: $("input-invite"),
  inviteError: $("invite-error"),
  signinScreen: $("signin-screen"),
  profileScreen: $("profile-screen"),
  chatScreen: $("chat-screen"),
  btnGoogle: $("btn-google-signin"),
  formEmailSignin: $("form-email-signin"),
  inputEmail: $("input-email"),
  emailStatus: $("email-status"),
  signinError: $("signin-error"),
  formProfile: $("form-profile"),
  inputDisplayName: $("input-display-name"),
  inputAffiliation: $("input-affiliation"),
  userMenuName: $("user-menu-name"),
  btnUserMenu: $("btn-user-menu"),
  userMenuDropdown: $("user-menu-dropdown"),
  btnSignout: $("btn-signout"),
  btnEditProfile: $("btn-edit-profile"),
  dialogEditProfile: $("dialog-edit-profile"),
  formEditProfile: $("form-edit-profile"),
  profilePreviewAvatar: $("profile-preview-avatar"),
  editDisplayName: $("edit-display-name"),
  editAffiliation: $("edit-affiliation"),
  editPhotoUrl: $("edit-photo-url"),
  btnEditProfileCancel: $("btn-edit-profile-cancel"),
  editProfileError: $("edit-profile-error"),
  btnUploadAvatar: $("btn-upload-avatar"),
  inputAvatarFile: $("input-avatar-file"),
  btnClearAvatar: $("btn-clear-avatar"),
  btnToggleSidebar: $("btn-toggle-sidebar"),
  chatSidebar: $("chat-sidebar"),
  sidebarBackdrop: $("sidebar-backdrop"),
  btnDeleteChannel: $("btn-delete-channel"),
  channelList: $("channel-list"),
  teamList: $("team-list"),
  dmList: $("dm-list"),
  btnNewChannel: $("btn-new-channel"),
  btnBrowseChannels: $("btn-browse-channels"),
  dialogBrowseChannels: $("dialog-browse-channels"),
  browseChannelsList: $("browse-channels-list"),
  btnBrowseChannelsClose: $("btn-browse-channels-close"),
  btnNewTeam: $("btn-new-team"),
  btnNewDm: $("btn-new-dm"),
  dialogNewChannel: $("dialog-new-channel"),
  formNewChannel: $("form-new-channel"),
  inputChannelName: $("input-channel-name"),
  inputChannelDesc: $("input-channel-desc"),
  btnNewChannelCancel: $("btn-new-channel-cancel"),
  newChannelError: $("new-channel-error"),
  dialogNewTeam: $("dialog-new-team"),
  formNewTeam: $("form-new-team"),
  inputTeamName: $("input-team-name"),
  inputTeamDesc: $("input-team-desc"),
  teamMemberList: $("team-member-list"),
  btnNewTeamCancel: $("btn-new-team-cancel"),
  newTeamError: $("new-team-error"),
  dialogNewDm: $("dialog-new-dm"),
  formNewDm: $("form-new-dm"),
  dmMemberList: $("dm-member-list"),
  btnNewDmCancel: $("btn-new-dm-cancel"),
  btnNewDmStart: $("btn-new-dm-start"),
  newDmError: $("new-dm-error"),
  dmFilter: $("dm-filter"),
  dialogNewReaction: $("dialog-new-reaction"),
  formNewReaction: $("form-new-reaction"),
  inputReactionEmoji: $("input-reaction-emoji"),
  inputReactionLabel: $("input-reaction-label"),
  inputReactionColor: $("input-reaction-color"),
  btnNewReactionCancel: $("btn-new-reaction-cancel"),
  newReactionError: $("new-reaction-error"),
  currentChannelName: $("current-channel-name"),
  btnMembers: $("btn-members"),
  memberCount: $("member-count"),
  dialogMembers: $("dialog-members"),
  formMembers: $("form-members"),
  membersTitle: $("members-title"),
  membersHint: $("members-hint"),
  membersList: $("members-list"),
  addMemberList: $("add-member-list"),
  btnMembersClose: $("btn-members-close"),
  // Pinned / search / typing
  pinnedBar: $("pinned-bar"),
  btnPinnedToggle: $("btn-pinned-toggle"),
  pinnedCount: $("pinned-count"),
  pinnedList: $("pinned-list"),
  searchBar: $("search-bar"),
  inputSearch: $("input-search"),
  searchCount: $("search-count"),
  btnSearchClose: $("btn-search-close"),
  btnSearch: $("btn-search"),
  typingIndicator: $("typing-indicator"),
  // Thread
  threadPanel: $("thread-panel"),
  btnThreadClose: $("btn-thread-close"),
  threadParent: $("thread-parent"),
  threadReplies: $("thread-replies"),
  formThreadCompose: $("form-thread-compose"),
  inputThreadMessage: $("input-thread-message"),
  // Switcher
  dialogSwitcher: $("dialog-channel-switcher"),
  inputSwitcher: $("input-switcher"),
  switcherResults: $("switcher-results"),
  // Bookmarks
  btnShowBookmarks: $("btn-show-bookmarks"),
  btnAdminPanel: $("btn-admin-panel"),
  dialogAdmin: $("dialog-admin"),
  adminList: $("admin-list"),
  inputNewAdmin: $("input-new-admin"),
  btnAddAdmin: $("btn-add-admin"),
  btnAdminClose: $("btn-admin-close"),
  inputNewInvite: $("input-new-invite"),
  btnRotateInvite: $("btn-rotate-invite"),
  inviteRotateStatus: $("invite-rotate-status"),
  maintenanceBanner: $("maintenance-banner"),
  maintenanceMessage: $("maintenance-message"),
  toggleMaintenance: $("toggle-maintenance"),
  toggleSigninLockdown: $("toggle-signin-lockdown"),
  inputMaintenanceMsg: $("input-maintenance-msg"),
  btnPauseNow: $("btn-pause-now"),
  btnResumeNow: $("btn-resume-now"),
  maintenanceStatus: $("maintenance-status"),
  btnExportImageList: $("btn-export-image-list"),
  imageListStatus: $("image-list-status"),
  imageListOutput: $("image-list-output"),
  adminError: $("admin-error"),
  bansList: $("bans-list"),
  // User profile (view) dialog
  dialogUserProfile: $("dialog-user-profile"),
  userProfileAvatar: $("user-profile-avatar"),
  userProfileName: $("user-profile-name"),
  userProfileOnline: $("user-profile-online"),
  userProfileAffiliation: $("user-profile-affiliation"),
  userProfileEmail: $("user-profile-email"),
  btnUserProfileDm: $("btn-user-profile-dm"),
  btnUserProfileEdit: $("btn-user-profile-edit"),
  btnUserProfileClose: $("btn-user-profile-close"),
  dialogBookmarks: $("dialog-bookmarks"),
  bookmarksList: $("bookmarks-list"),
  btnBookmarksClose: $("btn-bookmarks-close"),
  // Export
  btnExportChannel: $("btn-export-channel"),
  // Notification prefs
  btnNotifPrefs: $("btn-notif-prefs"),
  dialogNotifPrefs: $("dialog-notif-prefs"),
  formNotifPrefs: $("form-notif-prefs"),
  btnNotifPrefsCancel: $("btn-notif-prefs-cancel"),
  notifDm: $("notif-dm"),
  notifMention: $("notif-mention"),
  notifChannel: $("notif-channel"),
  notifReactions: $("notif-reactions"),
  notifAll: $("notif-all"),
  notifEmailMention: $("notif-email-mention"),
  notifEmailDm: $("notif-email-dm"),
  notifEmailChannel: $("notif-email-channel"),
  webhookSlack: $("webhook-slack"),
  webhookTeams: $("webhook-teams"),
  webhookDiscord: $("webhook-discord"),
  webhookDm: $("webhook-dm"),
  webhookMention: $("webhook-mention"),
  webhookChannel: $("webhook-channel"),
  webhookAllReplies: $("webhook-all-replies"),
  webhookAll: $("webhook-all"),
  btnWebhookTest: $("btn-webhook-test"),
  webhookTestStatus: $("webhook-test-status"),
  // Mentions view
  btnMentionsView: $("btn-mentions-view"),
  mentionsTotal: $("mentions-total"),
  dialogMentionsView: $("dialog-mentions"),
  mentionsViewList: $("mentions-view-list"),
  btnMentionsViewClose: $("btn-mentions-close"),
  // Poll
  btnPoll: $("btn-poll"),
  dialogPoll: $("dialog-poll"),
  formPoll: $("form-poll"),
  pollQuestion: $("poll-question"),
  pollOptions: $("poll-options"),
  pollMulti: $("poll-multi"),
  btnPollCancel: $("btn-poll-cancel"),
  pollError: $("poll-error"),
  messagesLoading: $("messages-loading"),
  messagesEmpty: $("messages-empty"),
  messagesList: $("messages-list"),
  messagesContainer: $("messages-container"),
  formCompose: $("form-compose"),
  inputMessage: $("input-message"),
  btnAttach: $("btn-attach"),
  inputFile: $("input-file"),
  attachmentPreview: $("attachment-preview"),
  attachmentThumb: $("attachment-thumb"),
  attachmentName: $("attachment-name"),
  btnAttachmentRemove: $("btn-attachment-remove"),
  uploadStatus: $("upload-status"),
  mentionDropdown: $("mention-dropdown"),
  notificationBanner: $("notification-banner"),
  btnEnableNotify: $("btn-enable-notify"),
  btnDismissNotify: $("btn-dismiss-notify"),
  lightbox: $("lightbox"),
  lightboxImg: $("lightbox-img"),
  btnLightboxClose: $("btn-lightbox-close"),
  fatalError: $("fatal-error"),
};

// ===========================================================================
// State
// ===========================================================================

const state = {
  user: null,
  userDoc: null,              // users/{uid} Firestore doc data
  channels: [],               // array of { id, ...data } — merged public + private + dm
  currentChannelId: null,
  unsubPublicChannels: null,
  unsubPrivateChannels: null,
  unsubUsers: null,
  unsubMessages: null,
  pendingAttachment: null,    // { file, blob, width, height }
  lastReadByChannel: {},      // mirror of userDoc
  channelLastMsgTs: {},       // last message ts per channel for unread calc
  allUsers: [],               // array of { uid, ...users/{uid} data }
  customReactions: [],        // from config/reactions.custom
  unsubReactions: null,
  // New: search, thread, bookmarks, polls, presence, typing
  searchQuery: "",
  currentMessages: [],        // last rendered docs
  threadMsgId: null,
  threadParent: null,
  unsubThreadReplies: null,
  typingByChannel: new Map(), // channelId -> { uid -> ts }
  unsubTyping: null,
  typingWriteTimer: null,
  heartbeatTimer: null,
  pendingScrollToMsg: null,   // msg id to scroll-to after messages load (permalink)
  forceScrollToBottom: false, // set true on channel switch so first render pins to latest message
  recentMessagesByChannel: new Map(),  // channelId -> array of recent message docs
  unreadMentionsByChannel: new Map(),  // channelId -> count of unread mentions for me
  draftsByChannel: new Map(),          // channelId -> in-progress composer text
  userSecrets: null,          // userSecrets/{uid} — private (webhooks, etc.) — not visible to others
  unsubSecrets: null,
  adminEmails: [],            // from config/admins.emails
  unsubAdmins: null,
  bans: { uids: [], emails: [] }, // from config/bans
  unsubBans: null,
  maintenance: { active: false, signInDisabled: false, message: "" }, // from config/maintenance
  unsubMaintenance: null,
  showHiddenDms: false,        // toggle: include user-hidden DMs in sidebar
  dmFilter: "",                // sidebar DM filter substring (case-insensitive)
};

function isAdmin() {
  return state.user && state.adminEmails.includes((state.user.email || "").toLowerCase());
}

function isBanned() {
  if (!state.user) return false;
  const email = (state.user.email || "").toLowerCase();
  return (state.bans.uids || []).includes(state.user.uid) ||
         (state.bans.emails || []).includes(email);
}

// True if this message should be invisible to the current user. Admin-hidden
// messages are silently filtered out for non-admins (no placeholder shown);
// admins still see the original with strikethrough + Restore.
function isHiddenForViewer(m) {
  if (!m || !m.deleted) return false;
  if (m.deletedByUid && m.deletedByUid !== m.authorUid) {
    // Hidden by admin — only admins can see it.
    return !isAdmin();
  }
  return false;
}

// Base tab title, preserved so the unread badge can prefix it.
const BASE_TAB_TITLE = document.title;

// ===========================================================================
// UI helpers
// ===========================================================================

function showScreen(name) {
  if (el.inviteScreen) el.inviteScreen.hidden = name !== "invite";
  el.signinScreen.hidden = name !== "signin";
  el.profileScreen.hidden = name !== "profile";
  el.chatScreen.hidden = name !== "chat";
}

function showFatal(msg) {
  el.fatalError.textContent = msg;
  el.fatalError.hidden = false;
}
function clearFatal() { el.fatalError.hidden = true; }

function showSigninError(msg) {
  el.signinError.textContent = msg;
  el.signinError.hidden = false;
}
function clearSigninError() { el.signinError.hidden = true; }

function escHtml(s) {
  return String(s).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;").replace(/'/g, "&#39;");
}

// ---------------------------------------------------------------------------
// Inline line icons (Lucide, MIT). Replaces emoji for a cleaner modern look.
// `icon(name, { filled, size, className })` returns an SVG string. Color
// inherits from currentColor; size defaults to 18px.
// ---------------------------------------------------------------------------
const LUCIDE_ICONS = {
  menu: '<line x1="4" x2="20" y1="12" y2="12"/><line x1="4" x2="20" y1="6" y2="6"/><line x1="4" x2="20" y1="18" y2="18"/>',
  users: '<path d="M16 21v-2a4 4 0 0 0-4-4H6a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4"/><path d="M22 21v-2a4 4 0 0 0-3-3.87"/><path d="M16 3.13a4 4 0 0 1 0 7.75"/>',
  trash: '<path d="M3 6h18"/><path d="M19 6v14c0 1-1 2-2 2H7c-1 0-2-1-2-2V6"/><path d="M8 6V4c0-1 1-2 2-2h4c1 0 2 1 2 2v2"/><line x1="10" x2="10" y1="11" y2="17"/><line x1="14" x2="14" y1="11" y2="17"/>',
  "chevron-down": '<path d="m6 9 6 6 6-6"/>',
  shield: '<path d="M20 13c0 5-3.5 7.5-7.66 8.95a1 1 0 0 1-.67-.01C7.5 20.5 4 18 4 13V6a1 1 0 0 1 1-1c2 0 4.5-1.2 6.24-2.72a1.17 1.17 0 0 1 1.52 0C14.51 3.81 17 5 19 5a1 1 0 0 1 1 1z"/>',
  bell: '<path d="M6 8a6 6 0 0 1 12 0c0 7 3 9 3 9H3s3-2 3-9"/><path d="M10.3 21a1.94 1.94 0 0 0 3.4 0"/>',
  search: '<circle cx="11" cy="11" r="8"/><path d="m21 21-4.35-4.35"/>',
  pin: '<line x1="12" x2="12" y1="17" y2="22"/><path d="M5 17h14v-1.76a2 2 0 0 0-1.11-1.79l-1.78-.9A2 2 0 0 1 15 10.76V6h1a2 2 0 0 0 0-4H8a2 2 0 0 0 0 4h1v4.76a2 2 0 0 1-1.11 1.79l-1.78.9A2 2 0 0 0 5 15.24Z"/>',
  paperclip: '<path d="M13.234 20.252 21 12.3"/><path d="m16 6-8.414 8.586a2 2 0 0 0 0 2.828 2 2 0 0 0 2.828 0l8.414-8.586a4 4 0 0 0 0-5.656 4 4 0 0 0-5.656 0l-8.415 8.585a6 6 0 1 0 8.486 8.486"/>',
  "smile-plus": '<path d="M22 11v1a10 10 0 1 1-9-10"/><path d="M8 14s1.5 2 4 2 4-2 4-2"/><line x1="9" x2="9.01" y1="9" y2="9"/><line x1="15" x2="15.01" y1="9" y2="9"/><path d="M16 5h6"/><path d="M19 2v6"/>',
  "message-square": '<path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"/>',
  bookmark: '<path d="m19 21-7-4-7 4V5a2 2 0 0 1 2-2h10a2 2 0 0 1 2 2v16z"/>',
  check: '<path d="M20 6 9 17l-5-5"/>',
  plus: '<path d="M5 12h14"/><path d="M12 5v14"/>',
  x: '<path d="M18 6 6 18"/><path d="m6 6 12 12"/>',
  "bar-chart-3": '<path d="M3 3v18h18"/><path d="M18 17V9"/><path d="M13 17V5"/><path d="M8 17v-3"/>',
  link: '<path d="M10 13a5 5 0 0 0 7.54.54l3-3a5 5 0 0 0-7.07-7.07l-1.72 1.71"/><path d="M14 11a5 5 0 0 0-7.54-.54l-3 3a5 5 0 0 0 7.07 7.07l1.71-1.71"/>',
};
function icon(name, opts = {}) {
  const body = LUCIDE_ICONS[name];
  if (!body) return "";
  const size = opts.size || 18;
  const cls = opts.className || "icon";
  const fill = opts.filled ? "currentColor" : "none";
  return `<svg class="${cls}" xmlns="http://www.w3.org/2000/svg" ` +
    `width="${size}" height="${size}" viewBox="0 0 24 24" ` +
    `fill="${fill}" stroke="currentColor" stroke-width="2" ` +
    `stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">${body}</svg>`;
}

// Simple URL linkifier + @mention renderer.
function mentionSlug(name) {
  return (name || "").trim().split(/\s+/)[0].toLowerCase().replace(/[^a-z0-9_\-]/g, "");
}
function findUserByMentionSlug(slug) {
  if (!slug) return null;
  const lower = slug.toLowerCase();
  return state.allUsers.find((u) => mentionSlug(u.displayName) === lower);
}
function parseMentions(text) {
  const uids = new Set();
  let broadcast = false;
  const re = /(^|\s)@([a-zA-Z0-9_\-]+)/g;
  let m;
  while ((m = re.exec(text)) !== null) {
    const word = m[2].toLowerCase();
    if (word === "channel") {
      broadcast = true;
    } else {
      const u = findUserByMentionSlug(word);
      if (u) uids.add(u.uid);
    }
  }
  return { mentionUids: [...uids], mentionsEveryone: broadcast };
}
// Renders a message body: Markdown formatting + URLs + @mentions.
// Returns { html, ytId } where ytId is a YouTube video id if detected (for embed).
function renderMessageBody(text) {
  let ytId = null;
  // 1) Protect fenced code blocks (```...```) — extract before any other processing.
  const blocks = [];
  let s = text.replace(/```([\s\S]*?)```/g, (_, code) => {
    const idx = blocks.length;
    blocks.push(code);
    return `\uE000BLOCK${idx}\uE001`;
  });
  // 2) Inline code (`...`)
  const inlineCodes = [];
  s = s.replace(/`([^`\n]+?)`/g, (_, code) => {
    const idx = inlineCodes.length;
    inlineCodes.push(code);
    return `\uE000INLINE${idx}\uE001`;
  });
  // 3) Escape HTML on the rest
  let html = escHtml(s);
  // 4) Bold and italic (non-greedy, simple)
  html = html.replace(/\*\*([^*\n][^*\n]*?)\*\*/g, "<strong>$1</strong>");
  html = html.replace(/(^|[^*])\*(?!\s)([^*\n]+?)\*(?!\*)/g, "$1<em>$2</em>");
  // 5) URLs
  html = html.replace(/\b((?:https?:\/\/|www\.)[^\s<>"')]+[^\s<>"')\.,;:!?])/gi, (url) => {
    const href = url.startsWith("www.") ? `https://${url}` : url;
    // Detect YouTube video id (first one wins for embed)
    if (!ytId) {
      const m = href.match(/(?:youtube\.com\/watch\?v=|youtu\.be\/|youtube\.com\/shorts\/)([A-Za-z0-9_\-]{6,})/);
      if (m) ytId = m[1];
    }
    // arXiv style link
    const ax = href.match(/arxiv\.org\/(?:abs|pdf)\/([0-9]{4}\.[0-9]{4,5})/i);
    if (ax) {
      return `<a class="arxiv-link" href="${href}" target="_blank" rel="noopener noreferrer">arXiv:${ax[1]}</a>`;
    }
    return `<a href="${href}" target="_blank" rel="noopener noreferrer">${url}</a>`;
  });
  // 6) @mentions
  html = html.replace(/(^|\s)@([a-zA-Z0-9_\-]+)/g, (full, pre, word) => {
    const lw = word.toLowerCase();
    if (lw === "channel") {
      return `${pre}<span class="mention-pill broadcast">@${escHtml(word)}</span>`;
    }
    const u = findUserByMentionSlug(word);
    if (u) {
      return `${pre}<span class="mention-pill" title="${escHtml(u.displayName)}">@${escHtml(word)}</span>`;
    }
    return full;
  });
  // 7) Restore inline code (escape inside)
  html = html.replace(/\uE000INLINE(\d+)\uE001/g, (_, i) =>
    `<code class="md-code">${escHtml(inlineCodes[+i])}</code>`,
  );
  // 8) Restore fenced code blocks
  html = html.replace(/\uE000BLOCK(\d+)\uE001/g, (_, i) =>
    `<pre class="md-pre">${escHtml(blocks[+i])}</pre>`,
  );
  // 9) Line breaks (convert \n to <br> outside <pre>)
  html = html.replace(/\n/g, "<br>");
  // But strip <br> inside <pre> (since <pre> preserves \n already — we already escaped \n->br, fix this)
  html = html.replace(/<pre class="md-pre">([\s\S]*?)<\/pre>/g, (m, inner) =>
    `<pre class="md-pre">${inner.replace(/<br>/g, "\n")}</pre>`,
  );
  return { html, ytId };
}

// Kept for compatibility where only text-to-html is needed.
function linkifyText(text) {
  return renderMessageBody(text).html;
}

// Build an avatar element for a user. Uses photoURL if present,
// otherwise a colored circle with the first letter of the name.
const AVATAR_COLORS = [
  "#667eea", "#764ba2", "#17a2b8", "#e67e22", "#27ae60",
  "#c0392b", "#d35400", "#2980b9", "#8e44ad", "#16a085",
];
function avatarColorFor(key) {
  let hash = 0;
  for (let i = 0; i < key.length; i++) hash = (hash * 31 + key.charCodeAt(i)) | 0;
  return AVATAR_COLORS[Math.abs(hash) % AVATAR_COLORS.length];
}
function renderAvatar(user, size) {
  const span = document.createElement("span");
  span.className = "avatar" + (size ? " avatar-" + size : "");
  const name = user?.displayName || user?.authorName || user?.email || "?";
  if (user?.photoURL) {
    const img = document.createElement("img");
    img.src = user.photoURL;
    img.alt = "";
    img.referrerPolicy = "no-referrer"; // Google photo URLs need this
    img.addEventListener("error", () => {
      // Fallback if the photo fails to load.
      img.remove();
      span.textContent = (name[0] || "?").toUpperCase();
      span.style.background = avatarColorFor(user?.uid || user?.authorUid || name);
    });
    span.appendChild(img);
  } else {
    span.textContent = (name[0] || "?").toUpperCase();
    span.style.background = avatarColorFor(user?.uid || user?.authorUid || name);
  }
  // Online presence dot (green) if lastSeenAt is recent.
  // Look up fresh lastSeenAt from allUsers cache so we don't rely on the caller.
  const fresh = getUserByUid(user?.uid);
  if (isUserOnline(fresh || user)) span.classList.add("online");
  return span;
}

function getUserByUid(uid) {
  return state.allUsers.find((u) => u.uid === uid);
}

function formatTime(ts) {
  if (!ts) return "";
  const d = ts.toDate ? ts.toDate() : new Date(ts);
  const now = new Date();
  const sameDay = d.toDateString() === now.toDateString();
  const opts = sameDay
    ? { hour: "2-digit", minute: "2-digit" }
    : { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit" };
  return d.toLocaleString(undefined, opts);
}

function autoResizeTextarea(ta) {
  ta.style.height = "auto";
  ta.style.height = Math.min(ta.scrollHeight, 160) + "px";
}

// ===========================================================================
// Auth flow
// ===========================================================================

el.btnGoogle.addEventListener("click", async () => {
  clearSigninError();
  el.btnGoogle.disabled = true;
  try {
    await signInWithPopup(auth, googleProvider);
  } catch (err) {
    console.error(err);
    showSigninError(friendlyAuthError(err));
  } finally {
    el.btnGoogle.disabled = false;
  }
});

function friendlyAuthError(err) {
  // beforeSignIn throws HttpsError → Firebase wraps as auth/internal-error or
  // auth/admin-restricted-operation. Message contains our text.
  const msg = err?.message || "";
  if (msg.includes("not on the BRS participant list") ||
      err?.code === "auth/admin-restricted-operation") {
    return "This email is not on the BRS participant list. Please contact the organizers.";
  }
  if (err?.code === "auth/popup-closed-by-user") {
    return "Sign-in cancelled.";
  }
  if (err?.code === "auth/network-request-failed") {
    return "Network error. Check your connection and try again.";
  }
  return err?.message || "Sign-in failed. Please try again.";
}

// --- Email magic link ---

const EMAIL_LINK_STORAGE_KEY = "brsChat.emailForSignIn";

el.formEmailSignin.addEventListener("submit", async (e) => {
  e.preventDefault();
  clearSigninError();
  const email = el.inputEmail.value.trim();
  if (!email) return;
  el.emailStatus.textContent = "Sending link…";
  try {
    const url = window.location.origin + window.location.pathname;
    await sendSignInLinkToEmail(auth, email, { url, handleCodeInApp: true });
    window.localStorage.setItem(EMAIL_LINK_STORAGE_KEY, email);
    el.emailStatus.textContent = `Sign-in link sent to ${email} from noreply@biohybrid-robotics.com. Check your inbox — and the spam / junk folder if it doesn't show up. Links expire in 60 minutes.`;
  } catch (err) {
    console.error(err);
    el.emailStatus.textContent = "";
    showSigninError(friendlyAuthError(err));
  }
});

async function handleEmailLinkReturn() {
  if (!isSignInWithEmailLink(auth, window.location.href)) return;
  let email = window.localStorage.getItem(EMAIL_LINK_STORAGE_KEY);
  if (!email) {
    email = window.prompt("Please re-enter your email to complete sign-in:");
    if (!email) return;
  }
  try {
    await signInWithEmailLink(auth, email, window.location.href);
    window.localStorage.removeItem(EMAIL_LINK_STORAGE_KEY);
    // Strip the auth params from URL.
    history.replaceState({}, "", window.location.pathname);
  } catch (err) {
    console.error(err);
    showSigninError(friendlyAuthError(err));
  }
}

// --- Sign out ---

el.btnSignout.addEventListener("click", async () => {
  try { await unregisterFCMToken(); } catch (_) {}
  await signOut(auth);
});

// --- Edit profile ---

el.btnEditProfile.addEventListener("click", () => {
  el.userMenuDropdown.hidden = true;
  el.btnUserMenu.setAttribute("aria-expanded", "false");
  openEditProfileDialog();
});

function openEditProfileDialog() {
  el.editDisplayName.value = state.userDoc?.displayName || "";
  el.editAffiliation.value = state.userDoc?.affiliation || "";
  el.editPhotoUrl.value = state.userDoc?.photoURL || state.user?.photoURL || "";
  el.editProfileError.hidden = true;
  updateProfilePreview();
  el.dialogEditProfile.showModal();
}

function updateProfilePreview() {
  el.profilePreviewAvatar.innerHTML = "";
  const av = renderAvatar({
    uid: state.user.uid,
    displayName: el.editDisplayName.value.trim() || state.userDoc?.displayName,
    photoURL: el.editPhotoUrl.value.trim() || null,
  });
  el.profilePreviewAvatar.appendChild(av);
}
el.editDisplayName.addEventListener("input", updateProfilePreview);
el.editPhotoUrl.addEventListener("input", updateProfilePreview);

// Upload + compress avatar → data URL in the photoURL field.
el.btnUploadAvatar.addEventListener("click", () => el.inputAvatarFile.click());
el.inputAvatarFile.addEventListener("change", async () => {
  const file = el.inputAvatarFile.files?.[0];
  el.inputAvatarFile.value = "";
  if (!file) return;
  if (!file.type.startsWith("image/")) {
    alert("Please choose an image file.");
    return;
  }
  try {
    el.btnUploadAvatar.textContent = "Processing…";
    el.btnUploadAvatar.disabled = true;
    const dataUrl = await compressToSquareDataUrl(file, 192, 0.85);
    el.editPhotoUrl.value = dataUrl;
    updateProfilePreview();
  } catch (err) {
    alert("Could not process image: " + err.message);
  } finally {
    el.btnUploadAvatar.textContent = "Upload image…";
    el.btnUploadAvatar.disabled = false;
  }
});
el.btnClearAvatar.addEventListener("click", () => {
  el.editPhotoUrl.value = "";
  updateProfilePreview();
});

// Crop to square center then shrink to `size`x`size` JPEG as a data URL.
async function compressToSquareDataUrl(file, size, quality) {
  const bitmap = await createImageBitmap(file, { imageOrientation: "from-image" });
  const s = Math.min(bitmap.width, bitmap.height);
  const sx = (bitmap.width - s) / 2;
  const sy = (bitmap.height - s) / 2;
  const canvas = document.createElement("canvas");
  canvas.width = size;
  canvas.height = size;
  const ctx = canvas.getContext("2d");
  ctx.drawImage(bitmap, sx, sy, s, s, 0, 0, size, size);
  bitmap.close?.();
  return canvas.toDataURL("image/jpeg", quality);
}

el.btnEditProfileCancel.addEventListener("click", () => el.dialogEditProfile.close());
el.formEditProfile.addEventListener("submit", async (e) => {
  e.preventDefault();
  const displayName = el.editDisplayName.value.trim();
  const affiliation = el.editAffiliation.value.trim();
  const photoURL = el.editPhotoUrl.value.trim() || null;
  if (!displayName) {
    el.editProfileError.textContent = "Display name is required.";
    el.editProfileError.hidden = false;
    return;
  }
  if (photoURL && !/^(https?:\/\/|data:image\/)/i.test(photoURL)) {
    el.editProfileError.textContent = "URL must start with http://, https://, or be an uploaded image.";
    el.editProfileError.hidden = false;
    return;
  }
  try {
    await updateDoc(doc(db, "users", state.user.uid), {
      displayName, affiliation, photoURL,
      lastSeenAt: serverTimestamp(),
    });
    state.userDoc = { ...state.userDoc, displayName, affiliation, photoURL };
    renderUserMenu();
    el.dialogEditProfile.close();
  } catch (err) {
    el.editProfileError.textContent = err.message;
    el.editProfileError.hidden = false;
  }
});

// --- User menu dropdown ---

el.btnUserMenu.addEventListener("click", () => {
  const expanded = el.btnUserMenu.getAttribute("aria-expanded") === "true";
  el.btnUserMenu.setAttribute("aria-expanded", !expanded);
  el.userMenuDropdown.hidden = expanded;
});
document.addEventListener("click", (e) => {
  if (!el.btnUserMenu.contains(e.target) && !el.userMenuDropdown.contains(e.target)) {
    el.btnUserMenu.setAttribute("aria-expanded", "false");
    el.userMenuDropdown.hidden = true;
  }
});

// ===========================================================================
// Auth state observer
// ===========================================================================

async function processAuthState(user) {
  state.user = user;
  // Wait for the invite gate to be evaluated before showing any auth UI.
  await gateResolved;
  if (!user) {
    teardownChatSubscriptions();
    if (gateState === "open") showScreen("signin");
    // If gate is closed, the invite screen is already shown — don't override.
    return;
  }
  // Authed users still need the gate cleared. (e.g. they were already signed
  // in from a previous session but the admin rotated the invite password.)
  if (gateState === "closed") return;

  // Confirm the blocking function ran and granted the claim.
  // In Phase 0 (Spark, no Functions), this check is skipped — any authenticated
  // user can proceed. The allowlist is enforced again in Phase A.
  if (!PHASE_0) {
    const token = await user.getIdTokenResult(true);
    if (!token.claims.brsMember) {
      await signOut(auth);
      showSigninError("Your account is not on the BRS participant list.");
      return;
    }
  }

  // Ban check (Phase 0: client-side; Phase A: also enforced in beforeSignIn).
  // Read config/bans once before any UI shows, so banned users see only the
  // sign-in screen with an explanation.
  try {
    const bansSnap = await getDoc(doc(db, "config", "bans"));
    const bansData = bansSnap.data() || {};
    state.bans = {
      uids: Array.isArray(bansData.uids) ? bansData.uids : [],
      emails: Array.isArray(bansData.emails)
        ? bansData.emails.map((e) => (e || "").toLowerCase()).filter(Boolean)
        : [],
    };
    if (isBanned()) {
      await signOut(auth);
      showSigninError(
        "Your account has been removed from BRS Chat by an administrator. " +
        "Contact the organizers if you think this is a mistake."
      );
      return;
    }
  } catch (e) {
    // Non-fatal: if config/bans doesn't exist yet, treat as no bans.
    console.warn("ban check failed (treating as no bans)", e);
  }

  // Load or create profile.
  const userRef = doc(db, "users", user.uid);
  const snap = await getDoc(userRef);
  if (!snap.exists() || !snap.data().displayName) {
    showScreen("profile");
    el.inputDisplayName.value = user.displayName || "";
    return;
  }
  state.userDoc = snap.data();
  state.lastReadByChannel = state.userDoc.lastReadByChannel || {};
  // Keep photoURL in sync with current Google profile (it may change).
  if (user.photoURL && state.userDoc.photoURL !== user.photoURL) {
    updateDoc(userRef, { photoURL: user.photoURL }).catch((e) =>
      console.warn("photoURL sync failed", e));
    state.userDoc.photoURL = user.photoURL;
  }
  renderUserMenu();

  showScreen("chat");
  initChat();
  startHeartbeat();
  handlePermalinkInHash();
}

onAuthStateChanged(auth, processAuthState);

el.formProfile.addEventListener("submit", async (e) => {
  e.preventDefault();
  const displayName = el.inputDisplayName.value.trim();
  const affiliation = el.inputAffiliation.value.trim();
  if (!displayName) return;
  const user = state.user;
  const userRef = doc(db, "users", user.uid);
  await setDoc(userRef, {
    email: user.email,
    displayName,
    affiliation,
    photoURL: user.photoURL || null,
    lastSeenAt: serverTimestamp(),
    lastReadByChannel: {},
  }, { merge: true });
  state.userDoc = {
    email: user.email, displayName, affiliation,
    photoURL: user.photoURL || null, lastReadByChannel: {},
  };
  renderUserMenu();
  showScreen("chat");
  initChat();
  startHeartbeat();
});

function renderUserMenu() {
  el.userMenuName.innerHTML = "";
  if (state.userDoc) {
    const avatar = renderAvatar({
      uid: state.user.uid,
      displayName: state.userDoc.displayName,
      photoURL: state.userDoc.photoURL || state.user.photoURL,
    });
    el.userMenuName.appendChild(avatar);
    const label = document.createElement("span");
    label.textContent = state.userDoc.displayName;
    el.userMenuName.appendChild(label);
  }
}

// ===========================================================================
// Chat init
// ===========================================================================

function initChat() {
  clearFatal();
  subscribeUsers();
  subscribeUserSecrets();
  subscribeChannels();
  subscribeCustomReactions();
  subscribeAdmins();
  subscribeBans();
  subscribeMaintenance();
  initNotifications();
  initFCM().then(registerFCMToken).catch((e) => console.warn("[fcm] init failed", e));
}

function teardownChatSubscriptions() {
  if (state.unsubPublicChannels) { state.unsubPublicChannels(); state.unsubPublicChannels = null; }
  if (state.unsubPrivateChannels) { state.unsubPrivateChannels(); state.unsubPrivateChannels = null; }
  if (state.unsubUsers) { state.unsubUsers(); state.unsubUsers = null; }
  if (state.unsubReactions) { state.unsubReactions(); state.unsubReactions = null; }
  if (state.unsubMessages) { state.unsubMessages(); state.unsubMessages = null; }
  if (state.unsubSecrets) { state.unsubSecrets(); state.unsubSecrets = null; }
  state.userSecrets = null;
  if (state.unsubAdmins) { state.unsubAdmins(); state.unsubAdmins = null; }
  state.adminEmails = [];
  if (state.unsubBans) { state.unsubBans(); state.unsubBans = null; }
  state.bans = { uids: [], emails: [] };
  if (state.unsubMaintenance) { state.unsubMaintenance(); state.unsubMaintenance = null; }
  state.maintenance = { active: false, signInDisabled: false, message: "" };
  applyMaintenanceUi();
  tearDownChannelNotifyListeners();
  if (state.unsubTyping) { state.unsubTyping(); state.unsubTyping = null; }
  stopHeartbeat();
  closeThread();
  generalJoinAttempted = false;
}

// --- Custom reactions (user-added stamps, shared) ---

function subscribeCustomReactions() {
  state.unsubReactions = onSnapshot(doc(db, "config", "reactions"), (snap) => {
    state.customReactions = (snap.data()?.custom || []).filter(
      (r) => r && r.key && r.emoji,
    );
  }, (err) => {
    console.warn("custom reactions subscription failed", err);
  });
}

function subscribeAdmins() {
  if (state.unsubAdmins) { state.unsubAdmins(); state.unsubAdmins = null; }
  state.unsubAdmins = onSnapshot(doc(db, "config", "admins"), (snap) => {
    const wasAdmin = isAdmin();
    const emails = (snap.data()?.emails || []).map((e) => (e || "").toLowerCase()).filter(Boolean);
    state.adminEmails = emails;
    // Toggle visibility of admin menu entry.
    if (el.btnAdminPanel) el.btnAdminPanel.hidden = !isAdmin();
    if (el.dialogMembers?.open) renderMembersDialog();
    if (el.dialogAdmin?.open) renderAdminDialog();
    // If my admin status flipped, re-render messages so admin-hidden ones
    // appear/disappear from view accordingly.
    if (wasAdmin !== isAdmin() && state.currentChannelId && state.currentMessages) {
      renderMessages(state.currentMessages, state.currentChannelId);
    }
  }, (err) => {
    console.warn("admins subscription failed", err);
  });
}

// Bans — list of uids + emails that are blocked from BRS Chat.
// In Phase 0 (no blocking function), this is enforced client-side: on sign-in
// we read this doc and force-signOut any banned user, then ensureJoinedGeneral
// also bails out. In Phase A, beforeSignIn will check the same doc server-side
// (this client check stays as defense-in-depth).
function subscribeBans() {
  if (state.unsubBans) { state.unsubBans(); state.unsubBans = null; }
  state.unsubBans = onSnapshot(doc(db, "config", "bans"), async (snap) => {
    const data = snap.data() || {};
    state.bans = {
      uids: Array.isArray(data.uids) ? data.uids : [],
      emails: Array.isArray(data.emails)
        ? data.emails.map((e) => (e || "").toLowerCase()).filter(Boolean)
        : [],
    };
    // If I just got banned (live, while signed in), force sign out.
    if (isBanned()) {
      console.warn("Banned account detected — signing out");
      try { await signOut(auth); } catch (_) {}
      showSigninError(
        "Your account has been removed from BRS Chat by an administrator. " +
        "Contact the organizers if you think this is a mistake."
      );
      return;
    }
    if (el.dialogAdmin?.open) renderAdminDialog();
    // Re-render so banned authors' avatars become non-clickable and any
    // orphan DMs (partner just got banned) get filtered out of the sidebar.
    renderChannelLists();
    if (state.currentChannelId && state.currentMessages) {
      renderMessages(state.currentMessages, state.currentChannelId);
    }
  }, (err) => {
    console.warn("bans subscription failed", err);
  });
}

// Maintenance — config/maintenance.{active, signInDisabled, message}.
// Read-only mode toggles a banner + disables the composer client-side
// (server-side enforcement is via firestore.rules canWrite() check).
function subscribeMaintenance() {
  if (state.unsubMaintenance) { state.unsubMaintenance(); state.unsubMaintenance = null; }
  state.unsubMaintenance = onSnapshot(doc(db, "config", "maintenance"), (snap) => {
    const data = snap.exists() ? (snap.data() || {}) : {};
    state.maintenance = {
      active: data.active === true,
      signInDisabled: data.signInDisabled === true,
      message: data.message || "",
    };
    applyMaintenanceUi();
    if (el.dialogAdmin?.open) renderMaintenanceControls();
  }, (err) => {
    console.warn("maintenance subscription failed", err);
  });
}

function applyMaintenanceUi() {
  const m = state.maintenance || {};
  if (el.maintenanceBanner) {
    el.maintenanceBanner.hidden = !m.active;
    if (el.maintenanceMessage) {
      el.maintenanceMessage.textContent = m.message ||
        "Chat is paused for maintenance. New messages cannot be sent.";
    }
  }
  // Disable composer + reply input when paused (admins keep theirs enabled
  // so they can post status updates and rotate keys).
  const lock = m.active && !isAdmin();
  const composer = document.getElementById("input-message");
  const sendBtn = document.getElementById("btn-send");
  const threadInput = document.getElementById("input-thread-message");
  if (composer) composer.disabled = lock;
  if (sendBtn) sendBtn.disabled = lock;
  if (threadInput) threadInput.disabled = lock;
}

function renderMaintenanceControls() {
  const m = state.maintenance || {};
  if (el.toggleMaintenance) el.toggleMaintenance.checked = !!m.active;
  if (el.toggleSigninLockdown) el.toggleSigninLockdown.checked = !!m.signInDisabled;
  if (el.inputMaintenanceMsg) el.inputMaintenanceMsg.value = m.message || "";
}

async function writeMaintenance(patch) {
  const data = {
    active: state.maintenance?.active || false,
    signInDisabled: state.maintenance?.signInDisabled || false,
    message: state.maintenance?.message || "",
    ...patch,
    updatedAt: serverTimestamp(),
    updatedByUid: state.user.uid,
  };
  await setDoc(doc(db, "config", "maintenance"), data, { merge: true });
}

// --- Users subscription (for member pickers + DM discovery) ---

function subscribeUsers() {
  state.unsubUsers = onSnapshot(collection(db, "users"), (snap) => {
    const next = snap.docs.map((d) => ({ uid: d.id, ...d.data() }));
    const profileChanged = didAnyProfileChange(state.allUsers || [], next);
    // Also detect my own hiddenDms changing so we can refresh the sidebar.
    const myUid = state.user?.uid;
    const oldMine = (state.allUsers || []).find((u) => u.uid === myUid);
    const newMine = next.find((u) => u.uid === myUid);
    const hiddenChanged = JSON.stringify(oldMine?.hiddenDms || []) !==
                         JSON.stringify(newMine?.hiddenDms || []);
    state.allUsers = next;
    // When someone edits their display name / photo, re-render the currently
    // visible messages so the new profile info propagates to older messages
    // (authorName/photoURL are denormalized onto each message but the render
    // prefers the live cache from getUserByUid).
    if (profileChanged && state.currentChannelId && state.currentMessages) {
      renderMessages(state.currentMessages, state.currentChannelId);
    }
    if (hiddenChanged) renderChannelLists();
  }, (err) => {
    console.warn("users subscription failed", err);
  });
}

// Compares the fields that affect rendered messages (displayName + photoURL).
// Presence/heartbeat updates (lastSeenAt) are ignored to avoid re-rendering
// the whole message list on every heartbeat.
function didAnyProfileChange(prev, next) {
  if (prev.length !== next.length) return true;
  const prevMap = new Map(prev.map((u) => [u.uid, u]));
  for (const n of next) {
    const p = prevMap.get(n.uid);
    if (!p) return true;
    if (p.displayName !== n.displayName) return true;
    if (p.photoURL !== n.photoURL) return true;
  }
  return false;
}

// --- Channels subscription ---
// We need two queries: (1) all public channels, (2) private channels where
// the current user is a member. Merge client-side. Firestore can't OR these
// in one query on Spark/Native mode without composite indexes.

function subscribeChannels() {
  // You only see channels you're a MEMBER of (public, team, or DM).
  // This means removing someone actually hides the channel for them.
  // #general auto-adds every signed-in user, so it's visible to everyone.
  // To discover public channels you're not in, use the Browse dialog.
  const q = query(
    collection(db, "channels"),
    where("members", "array-contains", state.user.uid),
  );
  state.unsubPrivateChannels = onSnapshot(q, (snap) => {
    state.channels = snap.docs.map((d) => ({ id: d.id, ...d.data() }))
      .sort((a, b) => {
        const am = a.createdAt?.toMillis?.() ?? 0;
        const bm = b.createdAt?.toMillis?.() ?? 0;
        return am - bm;
      });
    renderChannelLists();
    refreshChannelNotifyListeners();
    const cur = state.channels.find((c) => c.id === state.currentChannelId);
    if (cur) el.memberCount.textContent = (cur.members || []).length || "";
    if (el.dialogMembers.open) renderMembersDialog();
    if (el.dialogBrowseChannels?.open) renderBrowseChannels();
    ensureJoinedGeneral();
    // If user was removed from the currently viewed channel, fall back.
    if (state.currentChannelId && !state.channels.find((c) => c.id === state.currentChannelId)) {
      const fallback = state.channels.find((c) => (c.name || "").toLowerCase() === GENERAL_CHANNEL_NAME)
        || state.channels[0];
      if (fallback) selectChannel(fallback.id);
      else {
        state.currentChannelId = null;
        el.messagesList.innerHTML = "";
        el.currentChannelName.textContent = "(no channels)";
      }
    }
    if (!state.currentChannelId && state.channels.length > 0) {
      const first = state.channels.find((c) => (c.name || "").toLowerCase() === GENERAL_CHANNEL_NAME)
        || state.channels[0];
      selectChannel(first.id);
    }
  }, (err) => {
    console.error("channels subscription failed", err);
    showFatal("Failed to load channels: " + err.message);
  });
}

function renderChannelLists() {
  const pub = [];
  const teams = [];
  const dms = [];
  for (const ch of state.channels) {
    if (ch.archived) continue;
    if (ch.type === "dm") {
      // Hide orphaned DMs: every other party is gone (banned/removed) or the
      // members array is degenerate. Without this they show up as "(dm)".
      if (isOrphanDm(ch)) {
        leaveOrphanDm(ch);  // self-heal: remove me from members so it's gone for good
        continue;
      }
      dms.push(ch);
    }
    else if (ch.type === "team") teams.push(ch);
    else pub.push(ch);
  }
  // DMs sorted by recent activity (lastMessageAt desc, fallback createdAt).
  // Public/team channels stay in creation order so #general etc. don't move.
  dms.sort((a, b) => {
    const am = (a.lastMessageAt || a.createdAt)?.toMillis?.() ?? 0;
    const bm = (b.lastMessageAt || b.createdAt)?.toMillis?.() ?? 0;
    return bm - am;
  });
  // Split user-hidden DMs out so they only appear when the user has expanded
  // the "Show hidden" toggle.
  const hiddenSet = new Set(getMyHiddenDms());
  const visibleDms = [];
  const hiddenDms = [];
  for (const ch of dms) {
    if (hiddenSet.has(ch.id)) hiddenDms.push(ch);
    else visibleDms.push(ch);
  }
  // Optional sidebar filter — substring match against the comma-joined label.
  const f = (state.dmFilter || "").trim().toLowerCase();
  const matches = (ch) => !f || dmLabel(ch).toLowerCase().includes(f);
  const filteredVisibleDms = visibleDms.filter(matches);
  const filteredHiddenDms = hiddenDms.filter(matches);
  // Auto-show the filter input only when there are enough DMs to bother with.
  if (el.dmFilter) {
    el.dmFilter.hidden = (visibleDms.length + hiddenDms.length) < 5;
  }
  renderChannelSection(el.channelList, pub, "public");
  renderChannelSection(el.teamList, teams, "team");
  renderChannelSection(el.dmList, filteredVisibleDms, "dm");
  renderHiddenDmsFooter(filteredHiddenDms);
}

// Append a "Show N hidden DMs" / "Hide hidden DMs" toggle plus the hidden
// rows themselves (when expanded) to the bottom of the DM section.
function renderHiddenDmsFooter(hiddenDms) {
  if (!el.dmList) return;
  if (hiddenDms.length === 0) return;
  const footer = document.createElement("li");
  footer.className = "hidden-dms-toggle";
  footer.textContent = state.showHiddenDms
    ? `Hide ${hiddenDms.length} hidden`
    : `Show ${hiddenDms.length} hidden`;
  footer.addEventListener("click", () => {
    state.showHiddenDms = !state.showHiddenDms;
    renderChannelLists();
  });
  el.dmList.appendChild(footer);
  if (state.showHiddenDms) {
    for (const ch of hiddenDms) {
      const row = renderDmRow(ch, /* hidden= */ true);
      el.dmList.appendChild(row);
    }
  }
}

// Single DM row used by both the visible section (via renderChannelSection)
// and the expanded "hidden" group. Pulled out for symmetry.
function renderDmRow(ch, hidden) {
  const li = document.createElement("li");
  li.dataset.channelId = ch.id;
  if (hidden) li.classList.add("hidden-dm");
  if (ch.id === state.currentChannelId) li.classList.add("active");
  const others = getDmOthers(ch);
  const dmUser = others[0] || null;
  const label = dmLabel(ch);
  if (dmUser) li.appendChild(renderAvatar(dmUser, "xs"));
  const nameWrap = document.createElement("span");
  nameWrap.className = "dm-name";
  const nameEl = document.createElement("span");
  nameEl.className = "channel-name";
  nameEl.textContent = label;
  nameWrap.appendChild(nameEl);
  if (others.length > 1) {
    nameWrap.insertAdjacentHTML("beforeend",
      `<span class="group-dm-icon" aria-label="group">👥</span>`);
  }
  li.appendChild(nameWrap);
  // Hide / Unhide button (× for visible, ↶ for hidden).
  const xBtn = document.createElement("button");
  xBtn.type = "button";
  xBtn.className = "dm-hide-btn";
  xBtn.textContent = hidden ? "↶" : "×";
  xBtn.title = hidden ? "Unhide this DM" : "Hide this DM";
  xBtn.addEventListener("click", (e) => {
    e.stopPropagation();
    if (hidden) unhideDm(ch.id); else hideDm(ch.id);
  });
  li.appendChild(xBtn);
  li.addEventListener("click", () => selectChannel(ch.id));
  return li;
}

function isOrphanDm(ch) {
  if (ch?.type !== "dm") return false;
  const members = ch.members || [];
  if (members.length < 2) return true;  // only me left
  const others = members.filter((u) => u !== state.user.uid);
  if (others.length === 0) return true;
  // For group DMs, only orphan when EVERY other member is banned/missing.
  // (A 5-person group with 1 banned still works fine for the other 4.)
  if (others.every((u) => isUserBanned(u))) return true;
  return false;
}

const orphanDmCleanupAttempted = new Set();
function leaveOrphanDm(ch) {
  if (orphanDmCleanupAttempted.has(ch.id)) return;
  orphanDmCleanupAttempted.add(ch.id);
  // Remove self from the channel so subscribeChannels (where members
  // array-contains me) drops it from state.channels permanently.
  updateDoc(doc(db, "channels", ch.id), {
    members: arrayRemove(state.user.uid),
  }).catch((e) => console.warn("orphan DM cleanup failed", e));
}

function renderChannelSection(container, channels, kind) {
  container.innerHTML = "";
  for (const ch of channels) {
    const li = document.createElement("li");
    li.dataset.channelId = ch.id;
    if (ch.id === state.currentChannelId) li.classList.add("active");

    let label = ch.name;
    let prefix = "#";
    let lock = "";
    let dmUser = null;
    if (kind === "dm") {
      const others = getDmOthers(ch);
      dmUser = others[0] || null;  // for the avatar slot
      label = dmLabel(ch);
      prefix = "";
      // Group DM marker — small icon next to the name.
      if (others.length > 1) {
        lock = `<span class="group-dm-icon" aria-label="group">👥</span>`;
      }
    } else if (kind === "team") {
      lock = `<span class="lock-icon" aria-label="private">🔒</span>`;
    }

    if (prefix) {
      const hash = document.createElement("span");
      hash.className = "channel-hash";
      hash.textContent = prefix;
      li.appendChild(hash);
    } else if (dmUser) {
      li.appendChild(renderAvatar(dmUser, "xs"));
    }
    const nameWrap = document.createElement("span");
    nameWrap.className = "dm-name";
    const nameEl = document.createElement("span");
    nameEl.className = "channel-name";
    nameEl.textContent = label;
    nameWrap.appendChild(nameEl);
    if (lock) nameWrap.insertAdjacentHTML("beforeend", lock);
    li.appendChild(nameWrap);
    const badge = document.createElement("span");
    badge.className = "unread-badge";
    badge.hidden = true;
    badge.setAttribute("aria-label", "unread");
    li.appendChild(badge);

    const lastReadTs = state.lastReadByChannel[ch.id];
    const lastMsgTs = state.channelLastMsgTs[ch.id];
    const unread = lastMsgTs && (!lastReadTs || tsGt(lastMsgTs, lastReadTs)) && ch.id !== state.currentChannelId;
    if (unread) li.querySelector(".unread-badge").hidden = false;

    // Mention pill (higher priority than generic unread dot)
    const mentionCount = state.unreadMentionsByChannel.get(ch.id) || 0;
    if (mentionCount > 0 && ch.id !== state.currentChannelId) {
      const pill = document.createElement("span");
      pill.className = "mention-pill-count";
      pill.textContent = mentionCount > 9 ? "9+" : mentionCount;
      li.appendChild(pill);
      // hide the plain unread-badge since the pill is more informative
      li.querySelector(".unread-badge").hidden = true;
    }

    // × hide button — only on DM rows. Hover-only so it doesn't clutter.
    if (kind === "dm") {
      const xBtn = document.createElement("button");
      xBtn.type = "button";
      xBtn.className = "dm-hide-btn";
      xBtn.textContent = "×";
      xBtn.title = "Hide this DM";
      xBtn.addEventListener("click", (e) => {
        e.stopPropagation();
        hideDm(ch.id);
      });
      li.appendChild(xBtn);
    }

    li.addEventListener("click", () => selectChannel(ch.id));
    container.appendChild(li);
  }
}

function getDmOtherUser(ch) {
  const otherUid = (ch.members || []).find((u) => u !== state.user.uid);
  return state.allUsers.find((u) => u.uid === otherUid);
}

// All other members of a DM channel (excludes self). For group DMs (3+ people).
function getDmOthers(ch) {
  return (ch.members || [])
    .filter((u) => u !== state.user.uid)
    .map((u) => state.allUsers.find((x) => x.uid === u))
    .filter(Boolean);
}

// User-managed hide list — DMs the user clicked × on. Stored on
// users/{uid}.hiddenDms. Hiding only affects this user's sidebar; the
// channel doc + history are untouched and the other party is unaffected.
function getMyHiddenDms() {
  const me = getUserByUid(state.user?.uid);
  return Array.isArray(me?.hiddenDms) ? me.hiddenDms : [];
}

async function hideDm(channelId) {
  if (!state.user) return;
  try {
    await updateDoc(doc(db, "users", state.user.uid), {
      hiddenDms: arrayUnion(channelId),
    });
  } catch (err) {
    alert("Hide failed: " + err.message);
  }
}

async function unhideDm(channelId) {
  if (!state.user) return;
  try {
    await updateDoc(doc(db, "users", state.user.uid), {
      hiddenDms: arrayRemove(channelId),
    });
  } catch (err) {
    alert("Unhide failed: " + err.message);
  }
}

// Comma-joined display label for a DM channel (works for 1:1 and group DMs).
function dmLabel(ch) {
  const others = getDmOthers(ch);
  if (others.length === 0) return "(dm)";
  if (others.length === 1) return others[0].displayName || others[0].email || "(unknown)";
  const names = others.slice(0, 3).map((u) => u.displayName || u.email || "?").join(", ");
  return others.length > 3 ? `${names} +${others.length - 3}` : names;
}

function tsGt(a, b) {
  const am = a?.toMillis ? a.toMillis() : (a?.seconds ? a.seconds * 1000 : 0);
  const bm = b?.toMillis ? b.toMillis() : (b?.seconds ? b.seconds * 1000 : 0);
  return am > bm;
}

// --- Channel selection ---

function selectChannel(channelId) {
  // Save current draft before switching
  if (state.currentChannelId) {
    state.draftsByChannel.set(state.currentChannelId, el.inputMessage.value);
  }
  if (state.unsubMessages) { state.unsubMessages(); state.unsubMessages = null; }
  state.currentChannelId = channelId;
  // Restore draft for the new channel (or empty)
  el.inputMessage.value = state.draftsByChannel.get(channelId) || "";
  autoResizeTextarea(el.inputMessage);
  const ch = state.channels.find((c) => c.id === channelId);
  if (!ch) return;
  let titleText = ch.name;
  let titlePrefix = "#";
  if (ch.type === "dm") {
    titleText = dmLabel(ch);
    titlePrefix = getDmOthers(ch).length > 1 ? "" : "@";
  }
  el.currentChannelName.textContent = titleText;
  const titleHash = document.querySelector(".chat-title .channel-hash");
  if (titleHash) titleHash.textContent = titlePrefix;
  el.inputMessage.placeholder = `Message ${titlePrefix}${titleText} · Ctrl+Enter to send`;
  // Hide Members button for 1:1 DMs (always 2 people, not useful) but show
  // it for group DMs (3+) so members can be added/removed.
  const isGroupDm = ch.type === "dm" && (ch.members || []).length > 2;
  el.btnMembers.hidden = ch.type === "dm" && !isGroupDm;
  el.memberCount.textContent = (ch.members || []).length || "";
  // Show delete (archive) button only to creator or admin, and never for DM,
  // #general, or explicitly-flagged default channels.
  const isGeneral = (ch.name || "").toLowerCase() === GENERAL_CHANNEL_NAME;
  const canDelete = !!state.user && ch.type !== "dm" && !ch.isDefault && !isGeneral &&
    (ch.createdByUid === state.user.uid || isAdmin());
  if (el.btnDeleteChannel) el.btnDeleteChannel.hidden = !canDelete;
  renderChannelLists();
  el.messagesList.innerHTML = "";
  el.messagesLoading.hidden = false;
  el.messagesEmpty.hidden = true;
  state.forceScrollToBottom = true;

  // Close mobile sidebar after selection.
  closeMobileSidebar();

  subscribeMessages(channelId);
  subscribeTypingForCurrent();
  el.typingIndicator.hidden = true;
  markChannelRead(channelId);
}

function subscribeMessages(channelId) {
  const q = query(
    collection(db, "channels", channelId, "messages"),
    orderBy("createdAt", "desc"),
    limit(200),
  );
  state.unsubMessages = onSnapshot(q, (snap) => {
    el.messagesLoading.hidden = true;
    // Exclude thread replies from the main list.
    const docs = snap.docs
      .map((d) => ({ id: d.id, ...d.data() }))
      .filter((m) => !m.parentId)
      .reverse();
    state.currentMessages = docs;
    el.messagesEmpty.hidden = docs.length > 0;
    renderMessages(docs, channelId);
    renderPinnedBar(docs, channelId);
    if (docs.length > 0) {
      state.channelLastMsgTs[channelId] = docs[docs.length - 1].createdAt;
      if (channelId === state.currentChannelId) markChannelRead(channelId);
    }
    renderChannelLists();
    // If a permalink is queued for this channel, scroll to it.
    if (state.pendingScrollToMsg) {
      const targetId = state.pendingScrollToMsg;
      state.pendingScrollToMsg = null;
      requestAnimationFrame(() => scrollToMessage(targetId));
    }
  }, (err) => {
    console.error(err);
    showFatal("Failed to load messages: " + err.message);
  });
}

function renderMessages(docs, channelId) {
  const wasAtBottom = isScrolledToBottom(el.messagesContainer);
  const forceBottom = state.forceScrollToBottom;
  if (forceBottom) state.forceScrollToBottom = false;
  el.messagesList.innerHTML = "";
  const q = (state.searchQuery || "").trim().toLowerCase();
  let matches = 0;
  for (const m of docs) {
    if (isHiddenForViewer(m)) continue;
    if (q) {
      const hay = ((m.text || "") + " " + (m.authorName || "")).toLowerCase();
      if (!hay.includes(q)) continue;
      matches++;
    }
    el.messagesList.appendChild(renderMessage(m, channelId));
  }
  if (q) {
    el.searchCount.textContent = matches + " match" + (matches === 1 ? "" : "es");
  }
  if ((forceBottom || wasAtBottom) && !q) {
    requestAnimationFrame(() => scrollToBottom(el.messagesContainer));
  }
}

function scrollToMessage(msgId) {
  const li = el.messagesList.querySelector(`[data-message-id="${msgId}"]`);
  if (!li) return;
  li.scrollIntoView({ behavior: "smooth", block: "center" });
  li.classList.add("highlight");
  setTimeout(() => li.classList.remove("highlight"), 2000);
}

function isScrolledToBottom(container) {
  return container.scrollHeight - container.scrollTop - container.clientHeight < 80;
}
function scrollToBottom(container) {
  container.scrollTop = container.scrollHeight;
}

// ---------------------------------------------------------------------------
// Long-press tooltip — mobile-friendly equivalent of the native `title` hover.
// Tap = normal click behavior; press-and-hold ~500ms = show a tooltip bubble
// with the provided text until the next tap anywhere.
// ---------------------------------------------------------------------------
function hideLongPressTooltip() {
  document.querySelectorAll(".longpress-tooltip").forEach((t) => t.remove());
}
function showLongPressTooltipFor(anchor, text) {
  hideLongPressTooltip();
  if (!text) return;
  const tip = document.createElement("div");
  tip.className = "longpress-tooltip";
  tip.textContent = text;
  document.body.appendChild(tip);
  const rect = anchor.getBoundingClientRect();
  const top = Math.max(4, rect.top - tip.offsetHeight - 6) + window.scrollY;
  const left = Math.max(4, Math.min(
    window.innerWidth - tip.offsetWidth - 4,
    rect.left,
  )) + window.scrollX;
  tip.style.top = `${top}px`;
  tip.style.left = `${left}px`;
  setTimeout(() => {
    document.addEventListener("click", hideLongPressTooltip, { once: true });
    document.addEventListener("touchstart", hideLongPressTooltip, { once: true, passive: true });
  }, 0);
}
function attachLongPressTooltip(el, textFn) {
  let timer = null;
  let fired = false;
  const start = () => {
    fired = false;
    if (timer) clearTimeout(timer);
    timer = setTimeout(() => {
      fired = true;
      timer = null;
      const txt = typeof textFn === "function" ? textFn() : textFn;
      showLongPressTooltipFor(el, txt);
    }, 500);
  };
  const cancel = () => {
    if (timer) { clearTimeout(timer); timer = null; }
  };
  el.addEventListener("touchstart", start, { passive: true });
  el.addEventListener("touchend", cancel);
  el.addEventListener("touchmove", cancel, { passive: true });
  el.addEventListener("touchcancel", cancel);
  // Suppress the follow-up click that iOS/Android dispatch after a long press
  // so the reaction doesn't toggle when the user just wanted to see names.
  el.addEventListener("click", (e) => {
    if (fired) { e.stopImmediatePropagation(); e.preventDefault(); fired = false; }
  }, true);
}

function renderMessage(m, channelId) {
  const li = document.createElement("li");
  li.className = "message";
  li.dataset.messageId = m.id;
  const isOwn = m.authorUid === state.user.uid;
  const admin = isAdmin();
  if (isOwn) li.classList.add("own");
  if (m.deleted) li.classList.add("deleted");

  // Avatar: look up the author's current photoURL from the users cache,
  // falling back to the photoURL denormalized onto the message.
  const authorUser = getUserByUid(m.authorUid) || {
    uid: m.authorUid, displayName: m.authorName,
    photoURL: m.authorPhotoURL, email: m.authorEmail,
  };
  // Banned users' avatar/name are not clickable — can't open profile or DM.
  const authorClickable = !!m.authorUid && !isUserBanned(m.authorUid);
  const avatarEl = renderAvatar(authorUser);
  if (authorClickable) {
    avatarEl.classList.add("clickable");
    avatarEl.title = "View profile";
    avatarEl.addEventListener("click", () => showUserProfile(m.authorUid));
  }
  li.appendChild(avatarEl);

  const body = document.createElement("div");
  body.className = "message-body";
  li.appendChild(body);

  const meta = document.createElement("div");
  meta.className = "message-meta";
  meta.innerHTML =
    `<span class="message-author${authorClickable ? ' clickable' : ''}"></span>` +
    `<span class="message-affiliation"></span>` +
    `<span class="message-time"></span>` +
    (m.editedAt ? `<span class="message-edited">(edited)</span>` : "");
  // Prefer the live displayName/affiliation from the users cache so profile
  // renames propagate to already-rendered messages. Fall back to the values
  // denormalized onto the message at send time (for deleted users, etc.).
  const liveUser = getUserByUid(m.authorUid);
  const displayName = liveUser?.displayName || m.authorName || "unknown";
  const affiliation = liveUser?.affiliation ?? m.authorAffiliation ?? "";
  const email = liveUser?.email || m.authorEmail || "";
  const authorEl = meta.querySelector(".message-author");
  authorEl.textContent = displayName;
  if (m.authorUid) {
    authorEl.title = "View profile";
    authorEl.addEventListener("click", () => showUserProfile(m.authorUid));
  }
  if (affiliation) meta.querySelector(".message-affiliation").textContent = affiliation;
  meta.querySelector(".message-time").textContent = formatTime(m.createdAt);
  // Email shown as tooltip so users can verify identity even after a rename.
  if (email) meta.title = email;
  body.appendChild(meta);

  if (m.deleted) {
    // Two flavors:
    //  - "hidden by admin" (m.deletedByUid set, ≠ author) — text preserved so
    //    admin can Restore later. Non-admins see only the placeholder.
    //  - "deleted by author" (no deletedByUid, or === author) — gone for
    //    everyone (currently a hard delete, so this branch shouldn't fire).
    const hiddenByAdmin = m.deletedByUid && m.deletedByUid !== m.authorUid;
    if (!admin || !hiddenByAdmin) {
      const stub = document.createElement("div");
      stub.className = "deleted-stub";
      stub.textContent = hiddenByAdmin ? "(hidden by admin)" : "(deleted)";
      body.appendChild(stub);
      return li;
    }
    // Admin viewing an admin-hidden message: render full content faded so
    // they can decide to Restore. Non-admins are blocked above.
    const note = document.createElement("div");
    note.className = "deleted-stub";
    note.textContent = "(hidden by admin — only admins see the original)";
    body.appendChild(note);
    // fall through to the normal text/poll/etc. rendering, with the .deleted
    // class on <li> applying muted + strikethrough styling.
  }

  // Poll message?
  if (m.type === "poll" && m.poll) {
    body.appendChild(renderPollCard(channelId, m));
  } else if (m.text) {
    const text = document.createElement("div");
    text.className = "message-text";
    const { html, ytId } = renderMessageBody(m.text);
    text.innerHTML = html;
    body.appendChild(text);
    if (ytId) {
      const wrap = document.createElement("div");
      wrap.className = "yt-embed";
      const f = document.createElement("iframe");
      f.src = `https://www.youtube.com/embed/${ytId}`;
      f.loading = "lazy";
      f.allow = "accelerometer; encrypted-media; gyroscope; picture-in-picture";
      f.allowFullscreen = true;
      wrap.appendChild(f);
      body.appendChild(wrap);
    }
  }
  if (m.image && m.image.thumbUrl) {
    const img = document.createElement("img");
    img.className = "message-image";
    img.loading = "lazy";
    img.src = m.image.thumbUrl;
    img.alt = "attached image";
    img.addEventListener("click", () => openLightbox(m.image.url));
    body.appendChild(img);
  }

  // Replies summary
  if ((m.replyCount || 0) > 0) {
    const btn = document.createElement("button");
    btn.type = "button";
    btn.className = "replies-summary";
    btn.textContent = `🧵 ${m.replyCount} ${m.replyCount === 1 ? "reply" : "replies"}`;
    btn.addEventListener("click", () => openThread(channelId, m.id));
    body.appendChild(btn);
  }

  // Reactions
  const reactions = m.reactions || {};
  const reactionKeys = Object.keys(reactions).filter((e) => (reactions[e] || []).length > 0);
  if (reactionKeys.length > 0) {
    const wrap = document.createElement("div");
    wrap.className = "reactions";
    for (const key of reactionKeys) {
      const uids = reactions[key] || [];
      const meta = findReactionMeta(key);
      const pill = document.createElement("button");
      pill.type = "button";
      pill.className = "reaction-pill" + (uids.includes(state.user.uid) ? " mine" : "");
      if (meta.emoji) {
        pill.classList.add("reaction-badge");
        pill.style.setProperty("--badge-color", meta.color);
        pill.innerHTML =
          `<span class="reaction-emoji"></span>` +
          `<span class="reaction-label"></span>` +
          `<span class="reaction-count"></span>`;
        pill.querySelector(".reaction-emoji").textContent = meta.emoji;
        pill.querySelector(".reaction-label").textContent = meta.key;
      } else {
        pill.innerHTML = `<span>${key}</span><span class="reaction-count"></span>`;
      }
      pill.querySelector(".reaction-count").textContent = uids.length;
      const namesOf = () => uids
        .map((u) => getUserByUid(u)?.displayName || "?")
        .join(", ");
      pill.title = namesOf();
      attachLongPressTooltip(pill, namesOf);
      pill.addEventListener("click", () => toggleReaction(channelId, m, key, uids));
      wrap.appendChild(pill);
    }
    body.appendChild(wrap);
  }

  // Actions: react / reply / pin / save / copy-link / edit / delete.
  const actions = document.createElement("div");
  actions.className = "message-actions";

  const reactBtn = document.createElement("button");
  reactBtn.type = "button";
  reactBtn.innerHTML = icon("smile-plus", { size: 16 });
  reactBtn.title = "Add reaction";
  reactBtn.addEventListener("click", (e) => openEmojiPicker(e.currentTarget, channelId, m));
  actions.appendChild(reactBtn);

  // Reply (thread) — not available inside thread panel itself
  if (m.parentId == null) {
    const replyBtn = document.createElement("button");
    replyBtn.type = "button";
    replyBtn.innerHTML = icon("message-square", { size: 16 });
    replyBtn.title = "Reply in thread";
    replyBtn.addEventListener("click", () => openThread(channelId, m.id));
    actions.appendChild(replyBtn);
  }

  // Pin / Unpin
  const pinBtn = document.createElement("button");
  pinBtn.type = "button";
  pinBtn.innerHTML = icon("pin", { size: 16, filled: m.pinned });
  pinBtn.title = m.pinned ? "Unpin" : "Pin to channel";
  pinBtn.addEventListener("click", () => togglePin(channelId, m));
  actions.appendChild(pinBtn);

  // Save / Unsave
  const isSaved = !!(state.userDoc?.savedMessages || []).find(
    (s) => s.channelId === channelId && s.messageId === m.id,
  );
  const saveBtn = document.createElement("button");
  saveBtn.type = "button";
  saveBtn.innerHTML = icon("bookmark", { size: 16, filled: isSaved });
  saveBtn.title = isSaved ? "Unsave" : "Save for later";
  saveBtn.addEventListener("click", () => toggleSave(channelId, m));
  actions.appendChild(saveBtn);

  // Copy permalink
  const linkBtn = document.createElement("button");
  linkBtn.type = "button";
  linkBtn.innerHTML = icon("link", { size: 16 });
  linkBtn.title = "Copy link to message";
  linkBtn.addEventListener("click", () => copyPermalink(channelId, m.id));
  actions.appendChild(linkBtn);

  const now = Date.now();
  const createdMs = m.createdAt?.toMillis?.() ?? 0;
  if (isOwn && m.text && !m.deleted && (now - createdMs) < 5 * 60 * 1000) {
    const editBtn = document.createElement("button");
    editBtn.type = "button";
    editBtn.textContent = "Edit";
    editBtn.addEventListener("click", () => editMessage(channelId, m));
    actions.appendChild(editBtn);
  }
  // Delete:
  //  - Owner: hard-delete their own message (any time).
  //  - Admin (on someone else's message): soft-hide (preserves text so it
  //    can be Restored later by another admin).
  if (isOwn && !m.deleted) {
    const delBtn = document.createElement("button");
    delBtn.type = "button";
    delBtn.textContent = "Delete";
    delBtn.addEventListener("click", () => deleteMessage(channelId, m));
    actions.appendChild(delBtn);
  } else if (admin && !isOwn && !m.deleted) {
    const hideBtn = document.createElement("button");
    hideBtn.type = "button";
    hideBtn.textContent = "Hide";
    hideBtn.title = "Hide this message from non-admins (reversible)";
    hideBtn.addEventListener("click", () => hideMessage(channelId, m));
    actions.appendChild(hideBtn);
  } else if (admin && m.deleted && m.deletedByUid && m.deletedByUid !== m.authorUid) {
    // Hidden by admin — show Restore + permanent-delete buttons.
    const restoreBtn = document.createElement("button");
    restoreBtn.type = "button";
    restoreBtn.textContent = "Restore";
    restoreBtn.title = "Make this message visible to everyone again";
    restoreBtn.addEventListener("click", () => restoreMessage(channelId, m));
    actions.appendChild(restoreBtn);
    const purgeBtn = document.createElement("button");
    purgeBtn.type = "button";
    purgeBtn.textContent = "Delete permanently";
    purgeBtn.title = "Hard-delete this message — cannot be undone";
    purgeBtn.addEventListener("click", () => purgeMessage(channelId, m));
    actions.appendChild(purgeBtn);
  }
  body.appendChild(actions);

  return li;
}

// ---- Reactions (emoji + text badges) ----
// Each reaction is stored in Firestore as a key in messages/{mid}.reactions.
// Pure-emoji keys render as plain emoji pills; text badges render as colored
// pills with an emoji prefix, defined here.
const QUICK_REACTIONS = [
  { key: "👍" },
  { key: "❤️" },
  { key: "😂" },
  { key: "🎉" },
  { key: "🔥" },
  { key: "👀" },
  { key: "🙏" },
  { key: "🤔" },
  { key: "Yes" },
  { key: "No" },
  { key: "Thanks!", emoji: "🙏", color: "#27ae60" },
  { key: "Great!",  emoji: "✨", color: "#e67e22" },
  { key: "LGTM",    emoji: "✅", color: "#2980b9" },
  { key: "Nice!",   emoji: "👌", color: "#8e44ad" },
  { key: "Agree",   emoji: "🤝", color: "#16a085" },
  { key: "Wow!",    emoji: "🚀", color: "#c0392b" },
  { key: "I'm in!", emoji: "🙋", color: "#2980b9" },
];
function findReactionMeta(key) {
  return state.customReactions.find((r) => r.key === key)
    || QUICK_REACTIONS.find((r) => r.key === key)
    || { key };
}
function allReactions() {
  // Dedupe by key; custom reactions can override defaults if keys collide.
  const seen = new Set();
  const out = [];
  for (const r of [...QUICK_REACTIONS, ...state.customReactions]) {
    if (!seen.has(r.key)) { seen.add(r.key); out.push(r); }
  }
  return out;
}
let currentEmojiPicker = null;

function openEmojiPicker(anchorBtn, channelId, m) {
  closeEmojiPicker();
  const picker = document.createElement("div");
  picker.className = "emoji-picker";
  for (const r of allReactions()) {
    const btn = document.createElement("button");
    btn.type = "button";
    if (r.emoji && r.key !== r.emoji) {
      // Text badge style.
      btn.className = "badge-swatch";
      btn.style.background = r.color || "#667eea";
      btn.textContent = `${r.emoji} ${r.key}`;
    } else {
      btn.textContent = r.key;
    }
    btn.addEventListener("click", () => {
      const existing = (m.reactions && m.reactions[r.key]) || [];
      toggleReaction(channelId, m, r.key, existing);
      closeEmojiPicker();
    });
    picker.appendChild(btn);
  }
  // "+" button to add a new custom stamp.
  const addBtn = document.createElement("button");
  addBtn.type = "button";
  addBtn.className = "add-stamp";
  addBtn.textContent = "＋ Add stamp";
  addBtn.addEventListener("click", () => {
    closeEmojiPicker();
    openAddReactionDialog();
  });
  picker.appendChild(addBtn);
  // Position inside the actions container so it floats below.
  const actionsEl = anchorBtn.closest(".message-actions");
  actionsEl.style.position = "relative";
  actionsEl.appendChild(picker);
  currentEmojiPicker = picker;

  setTimeout(() => {
    document.addEventListener("click", onDocClickClose, { once: true });
  }, 0);
}
function onDocClickClose(e) {
  if (currentEmojiPicker && !currentEmojiPicker.contains(e.target)) {
    closeEmojiPicker();
  }
}
function closeEmojiPicker() {
  if (currentEmojiPicker) { currentEmojiPicker.remove(); currentEmojiPicker = null; }
}

function openAddReactionDialog() {
  el.inputReactionEmoji.value = "";
  el.inputReactionLabel.value = "";
  el.inputReactionColor.value = "#667eea";
  el.newReactionError.hidden = true;
  el.dialogNewReaction.showModal();
}
el.btnNewReactionCancel?.addEventListener("click", () => el.dialogNewReaction.close());
el.formNewReaction?.addEventListener("submit", async (e) => {
  e.preventDefault();
  const emoji = el.inputReactionEmoji.value.trim();
  const key = el.inputReactionLabel.value.trim();
  const color = el.inputReactionColor.value;
  if (!emoji) {
    el.newReactionError.textContent = "Please enter an emoji (e.g. 🎉).";
    el.newReactionError.hidden = false;
    return;
  }
  // If no label, the emoji itself is the key — renders as a plain emoji pill.
  const finalKey = key || emoji;
  if (allReactions().some((r) => r.key === finalKey)) {
    el.newReactionError.textContent = key
      ? "A stamp with that label already exists."
      : "That emoji is already a stamp.";
    el.newReactionError.hidden = false;
    return;
  }
  try {
    const newStamp = key
      ? { key: finalKey, emoji, color, addedBy: state.user.uid }
      : { key: finalKey, addedBy: state.user.uid };
    await setDoc(doc(db, "config", "reactions"), {
      custom: [...state.customReactions, newStamp],
    }, { merge: true });
    el.dialogNewReaction.close();
  } catch (err) {
    el.newReactionError.textContent = err.message;
    el.newReactionError.hidden = false;
  }
});

async function toggleReaction(channelId, m, emoji, currentUids) {
  const myUid = state.user.uid;
  const has = currentUids.includes(myUid);
  try {
    await updateDoc(msgDocRef(channelId, m), {
      [`reactions.${emoji}`]: has ? arrayRemove(myUid) : arrayUnion(myUid),
    });
  } catch (err) {
    console.error(err);
  }
}

// Returns the Firestore doc ref for a message, whether it's a top-level
// message or a reply living in the subcollection messages/{parent}/replies/{r}.
function msgDocRef(channelId, m) {
  if (m.parentId && m.id !== m.parentId) {
    return doc(db, "channels", channelId, "messages", m.parentId, "replies", m.id);
  }
  return doc(db, "channels", channelId, "messages", m.id);
}

async function editMessage(channelId, m) {
  const newText = window.prompt("Edit message:", m.text);
  if (newText === null || newText.trim() === m.text.trim()) return;
  try {
    await updateDoc(msgDocRef(channelId, m), {
      text: newText.trim(),
      editedAt: serverTimestamp(),
    });
  } catch (err) {
    alert("Edit failed: " + err.message);
  }
}

async function deleteMessage(channelId, m) {
  if (!window.confirm("Delete this message?")) return;
  try {
    await deleteDoc(msgDocRef(channelId, m));
  } catch (err) {
    alert("Delete failed: " + err.message);
  }
}

// Admin: hide someone else's message. Soft-delete — text is preserved on the
// document so another admin can Restore it later. Non-admin clients see only
// "(hidden by admin)".
async function hideMessage(channelId, m) {
  if (!isAdmin()) return;
  if (m.authorUid === state.user.uid) return;  // own messages use deleteMessage
  if (!window.confirm(
    `Hide this message by ${m.authorName}?\n\n` +
    `Non-admins will see "(hidden by admin)" in its place. ` +
    `Admins still see the original and can Restore it.`
  )) return;
  try {
    await updateDoc(msgDocRef(channelId, m), {
      deleted: true,
      deletedByUid: state.user.uid,
      deletedAt: serverTimestamp(),
    });
  } catch (err) {
    alert("Hide failed: " + err.message);
  }
}

// Admin: restore a previously hidden message.
async function restoreMessage(channelId, m) {
  if (!isAdmin()) return;
  if (!window.confirm("Restore this message? It will be visible to everyone again.")) return;
  try {
    await updateDoc(msgDocRef(channelId, m), {
      deleted: false,
      deletedByUid: deleteField(),
      deletedAt: deleteField(),
    });
  } catch (err) {
    alert("Restore failed: " + err.message);
  }
}

// Admin: permanently delete a hidden message (no recovery).
async function purgeMessage(channelId, m) {
  if (!isAdmin()) return;
  if (!window.confirm(
    "Permanently delete this message?\n\n" +
    "This cannot be undone. The message is gone for everyone, including admins."
  )) return;
  try {
    await deleteDoc(msgDocRef(channelId, m));
  } catch (err) {
    alert("Purge failed: " + err.message);
  }
}

// --- Mark channel read ---

let markReadTimer = null;
function markChannelRead(channelId) {
  clearTimeout(markReadTimer);
  markReadTimer = setTimeout(async () => {
    state.lastReadByChannel[channelId] = Timestamp.now();
    state.unreadMentionsByChannel.set(channelId, 0);
    updateTabTitleAndMentionBadge();
    try {
      await updateDoc(doc(db, "users", state.user.uid), {
        [`lastReadByChannel.${channelId}`]: serverTimestamp(),
        lastSeenAt: serverTimestamp(),
      });
    } catch (err) {
      console.warn("markRead failed", err);
    }
    renderChannelLists();
  }, 500);
}

// ===========================================================================
// Compose / send
// ===========================================================================

// ---- Mention autocomplete (supports multiple textareas) ----

let mentionState = {
  active: false, atPos: -1, cursorPos: -1, candidates: [], selected: 0, textarea: null,
};

// Enter=newline, Shift+Enter=send (user preference; plays nicer with Japanese IME).
function handleComposerKeydown(e, textarea, submitFn) {
  if (mentionState.active && mentionState.textarea === textarea && !el.mentionDropdown.hidden) {
    if (e.key === "ArrowDown") {
      e.preventDefault();
      mentionState.selected = (mentionState.selected + 1) % mentionState.candidates.length;
      renderMentionDropdown();
      return;
    }
    if (e.key === "ArrowUp") {
      e.preventDefault();
      mentionState.selected = (mentionState.selected - 1 + mentionState.candidates.length) % mentionState.candidates.length;
      renderMentionDropdown();
      return;
    }
    if (e.key === "Enter" || e.key === "Tab") {
      e.preventDefault();
      insertMention(mentionState.candidates[mentionState.selected]);
      return;
    }
    if (e.key === "Escape") {
      e.preventDefault();
      closeMentionAutocomplete();
      return;
    }
  }
  // Ctrl/Cmd+Enter sends, plain Enter inserts a newline.
  if (e.key === "Enter" && (e.ctrlKey || e.metaKey) && !e.isComposing) {
    e.preventDefault();
    submitFn();
  }
}

el.inputMessage.addEventListener("input", () => {
  autoResizeTextarea(el.inputMessage);
  updateMentionAutocomplete(el.inputMessage);
  onComposeTyping();
});
el.inputMessage.addEventListener("keydown", (e) =>
  handleComposerKeydown(e, el.inputMessage, () => el.formCompose.requestSubmit()));
el.inputMessage.addEventListener("blur", () => setTimeout(closeMentionAutocomplete, 150));

// Same wiring for thread reply composer.
el.inputThreadMessage.addEventListener("input", () => {
  autoResizeTextarea(el.inputThreadMessage);
  updateMentionAutocomplete(el.inputThreadMessage);
});
el.inputThreadMessage.addEventListener("keydown", (e) =>
  handleComposerKeydown(e, el.inputThreadMessage, () => el.formThreadCompose.requestSubmit()));
el.inputThreadMessage.addEventListener("blur", () => setTimeout(closeMentionAutocomplete, 150));

function updateMentionAutocomplete(textarea) {
  textarea = textarea || el.inputMessage;
  const t = textarea.value;
  const cursor = textarea.selectionStart;
  let atPos = -1;
  for (let i = cursor - 1; i >= 0; i--) {
    const c = t[i];
    if (c === "@") {
      if (i === 0 || /\s/.test(t[i - 1])) { atPos = i; }
      break;
    }
    if (/\s/.test(c)) break;
  }
  if (atPos < 0) return closeMentionAutocomplete();
  const query = t.slice(atPos + 1, cursor);
  if (!/^[a-zA-Z0-9_\-]*$/.test(query)) return closeMentionAutocomplete();

  const lower = query.toLowerCase();
  const userRows = state.allUsers
    .filter((u) => u.uid !== state.user.uid && u.displayName)
    .filter((u) => {
      const slug = mentionSlug(u.displayName);
      return slug.startsWith(lower) || (u.displayName || "").toLowerCase().includes(lower);
    })
    .slice(0, 8)
    .map((u) => ({
      user: u,
      token: "@" + mentionSlug(u.displayName),
      label: u.displayName,
      sub: [u.affiliation, u.email].filter(Boolean).join(" · "),
    }));
  const broadcasts = [
    { broadcast: true, key: "channel", label: "Notify everyone in this channel", token: "@channel" },
  ].filter((b) => b.key.startsWith(lower));

  const candidates = [...userRows, ...broadcasts];
  if (candidates.length === 0) return closeMentionAutocomplete();

  mentionState = { active: true, atPos, cursorPos: cursor, candidates, selected: 0, textarea };
  renderMentionDropdown();
  positionMentionDropdown(textarea);
}

function positionMentionDropdown(textarea) {
  const r = textarea.getBoundingClientRect();
  el.mentionDropdown.style.position = "fixed";
  el.mentionDropdown.style.left = r.left + "px";
  el.mentionDropdown.style.bottom = (window.innerHeight - r.top + 4) + "px";
  el.mentionDropdown.style.right = "auto";
  el.mentionDropdown.style.width = Math.min(r.width, 360) + "px";
}

function renderMentionDropdown() {
  el.mentionDropdown.innerHTML = "";
  mentionState.candidates.forEach((c, i) => {
    const row = document.createElement("div");
    row.className = "mention-row"
      + (i === mentionState.selected ? " active" : "")
      + (c.broadcast ? " broadcast" : "");
    row.setAttribute("role", "option");
    row.addEventListener("mousedown", (e) => {
      e.preventDefault();
      insertMention(c);
    });
    if (c.user) row.appendChild(renderAvatar(c.user, "xs"));
    const info = document.createElement("span");
    info.innerHTML = `<strong></strong><span class="mention-handle"></span>`;
    info.querySelector("strong").textContent = c.label;
    info.querySelector(".mention-handle").textContent =
      " " + c.token + (c.sub ? " · " + c.sub : "");
    row.appendChild(info);
    el.mentionDropdown.appendChild(row);
  });
  el.mentionDropdown.hidden = false;
}

function closeMentionAutocomplete() {
  mentionState.active = false;
  el.mentionDropdown.hidden = true;
}

function insertMention(c) {
  const textarea = mentionState.textarea || el.inputMessage;
  const t = textarea.value;
  const before = t.slice(0, mentionState.atPos);
  const after = t.slice(mentionState.cursorPos);
  const inserted = c.token + " ";
  textarea.value = before + inserted + after;
  const pos = before.length + inserted.length;
  textarea.setSelectionRange(pos, pos);
  textarea.focus();
  autoResizeTextarea(textarea);
  closeMentionAutocomplete();
}

el.formCompose.addEventListener("submit", async (e) => {
  e.preventDefault();
  const text = el.inputMessage.value.trim();
  if (!text && !state.pendingAttachment) return;
  if (!state.currentChannelId) return;

  const channelId = state.currentChannelId;
  const { mentionUids, mentionsEveryone } = parseMentions(text);
  const msg = {
    text: text || "",
    image: null,
    authorUid: state.user.uid,
    authorEmail: state.user.email,
    authorName: state.userDoc.displayName,
    authorAffiliation: state.userDoc.affiliation || "",
    mentions: mentionUids,
    mentionsEveryone,
    createdAt: serverTimestamp(),
    editedAt: null,
    deleted: false,
    backedUpAt: null,
  };

  el.inputMessage.value = "";
  state.draftsByChannel.delete(channelId);
  autoResizeTextarea(el.inputMessage);
  clearOwnTyping();

  try {
    if (state.pendingAttachment) {
      el.uploadStatus.textContent = "Uploading image…";
      msg.image = await uploadImage(state.pendingAttachment, channelId);
      clearAttachment();
      el.uploadStatus.textContent = "";
    }
    await addDoc(collection(db, "channels", channelId, "messages"), msg);
    // Bump channel's lastMessageAt so the sidebar can sort by recent activity.
    updateDoc(doc(db, "channels", channelId), {
      lastMessageAt: serverTimestamp(),
    }).catch((e) => console.warn("lastMessageAt bump failed", e));
  } catch (err) {
    console.error(err);
    el.uploadStatus.textContent = "";
    alert("Send failed: " + err.message);
    // Restore text so user doesn't lose their input.
    if (text) el.inputMessage.value = text;
  }
});

// ===========================================================================
// Image attach / upload
// ===========================================================================

el.btnAttach.addEventListener("click", () => el.inputFile.click());
el.inputFile.addEventListener("change", async () => {
  const file = el.inputFile.files?.[0];
  el.inputFile.value = "";
  if (!file) return;
  await attachFile(file);
});

// Drag & drop.
el.messagesContainer.addEventListener("dragover", (e) => {
  if (e.dataTransfer.types.includes("Files")) { e.preventDefault(); }
});
el.messagesContainer.addEventListener("drop", async (e) => {
  e.preventDefault();
  const file = e.dataTransfer.files?.[0];
  if (file) await attachFile(file);
});

el.btnAttachmentRemove.addEventListener("click", clearAttachment);

async function attachFile(file) {
  clearFatal();
  if (!file.type.startsWith("image/")) {
    alert("Only image files are supported.");
    return;
  }
  const isHeic = /hei[cf]/i.test(file.type) || /\.hei[cf]$/i.test(file.name);
  let blob = file;
  let width = 0, height = 0;

  if (!isHeic) {
    // Client-side compression via canvas for JPEG/PNG/WebP.
    try {
      const res = await compressImage(file, 1600, 0.82);
      blob = res.blob;
      width = res.width;
      height = res.height;
    } catch (err) {
      console.warn("compression failed, uploading original", err);
    }
  }
  // HEIC is sent as-is; Cloudinary's incoming transformation handles conversion.

  if (blob.size > 10 * 1024 * 1024) {
    alert("Image is too large (max 10MB).");
    return;
  }

  state.pendingAttachment = { file, blob, width, height };

  // Preview.
  const previewUrl = isHeic ? "" : URL.createObjectURL(blob);
  el.attachmentThumb.src = previewUrl;
  el.attachmentThumb.style.display = isHeic ? "none" : "";
  el.attachmentName.textContent = `${file.name} (${formatBytes(blob.size)})`;
  el.attachmentPreview.hidden = false;
}

function clearAttachment() {
  state.pendingAttachment = null;
  el.attachmentPreview.hidden = true;
  if (el.attachmentThumb.src) URL.revokeObjectURL(el.attachmentThumb.src);
  el.attachmentThumb.src = "";
}

function formatBytes(n) {
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(0)} KB`;
  return `${(n / 1024 / 1024).toFixed(1)} MB`;
}

async function compressImage(file, maxEdge, quality) {
  const bitmap = await createImageBitmap(file, { imageOrientation: "from-image" });
  let { width, height } = bitmap;
  const scale = Math.min(1, maxEdge / Math.max(width, height));
  width = Math.round(width * scale);
  height = Math.round(height * scale);
  const canvas = document.createElement("canvas");
  canvas.width = width;
  canvas.height = height;
  const ctx = canvas.getContext("2d");
  ctx.drawImage(bitmap, 0, 0, width, height);
  bitmap.close?.();
  const blob = await new Promise((resolve) =>
    canvas.toBlob(resolve, "image/jpeg", quality));
  return { blob, width, height };
}

async function uploadImage({ blob, file }, channelId) {
  // 1. Ask Cloud Function for a signed upload payload.
  const getSig = httpsCallable(functions, "getUploadSignature");
  const { data } = await getSig({ channelId });
  const { timestamp, folder, upload_preset, signature, apiKey, cloudName } = data;

  // 2. POST to Cloudinary.
  const form = new FormData();
  form.append("file", blob, file.name);
  form.append("api_key", apiKey);
  form.append("timestamp", String(timestamp));
  form.append("signature", signature);
  form.append("folder", folder);
  form.append("upload_preset", upload_preset);

  const res = await fetch(`https://api.cloudinary.com/v1_1/${cloudName}/image/upload`, {
    method: "POST",
    body: form,
  });
  if (!res.ok) {
    const txt = await res.text().catch(() => "");
    throw new Error(`Cloudinary upload failed (${res.status}): ${txt.slice(0, 200)}`);
  }
  const result = await res.json();

  // 3. Use the original URL as thumb. The upload preset's incoming
  // transformation already caps at 1920px with q_auto:good / f_auto, so the
  // original is already optimized; CSS scales it down to 300px for display.
  // Using the unmodified URL avoids Cloudinary's Strict Transformations
  // rejecting ad-hoc transform URLs (returns HTTP 401).
  const thumbUrl = result.secure_url;

  return {
    publicId: result.public_id,
    url: result.secure_url,
    thumbUrl,
    width: result.width,
    height: result.height,
  };
}

// ===========================================================================
// Lightbox
// ===========================================================================

function openLightbox(url) {
  el.lightboxImg.src = url;
  el.lightbox.showModal();
}
el.btnLightboxClose.addEventListener("click", () => el.lightbox.close());
el.lightbox.addEventListener("click", (e) => {
  // Close on backdrop click.
  if (e.target === el.lightbox) el.lightbox.close();
});
el.lightbox.addEventListener("close", () => { el.lightboxImg.src = ""; });

// ===========================================================================
// New channel dialog
// ===========================================================================

el.btnNewChannel.addEventListener("click", () => {
  el.inputChannelName.value = "";
  el.inputChannelDesc.value = "";
  el.newChannelError.hidden = true;
  el.dialogNewChannel.showModal();
});
el.btnNewChannelCancel.addEventListener("click", () => el.dialogNewChannel.close());
el.formNewChannel.addEventListener("submit", async (e) => {
  e.preventDefault();
  const name = el.inputChannelName.value.trim().toLowerCase();
  const description = el.inputChannelDesc.value.trim();
  if (!/^[a-z0-9][a-z0-9_-]{1,31}$/.test(name)) {
    el.newChannelError.textContent = "Invalid channel name.";
    el.newChannelError.hidden = false;
    return;
  }
  if (state.channels.some((c) => c.name === name && !c.archived)) {
    el.newChannelError.textContent = "A channel with that name already exists.";
    el.newChannelError.hidden = false;
    return;
  }
  try {
    const ref = await addDoc(collection(db, "channels"), {
      name,
      description,
      type: "public",
      members: [state.user.uid],
      createdByUid: state.user.uid,
      createdAt: serverTimestamp(),
      isDefault: false,
      archived: false,
    });
    el.dialogNewChannel.close();
    selectChannel(ref.id);
  } catch (err) {
    el.newChannelError.textContent = err.message;
    el.newChannelError.hidden = false;
  }
});

// ===========================================================================
// New team (private channel) dialog
// ===========================================================================

el.btnNewTeam.addEventListener("click", () => {
  el.inputTeamName.value = "";
  el.inputTeamDesc.value = "";
  el.newTeamError.hidden = true;
  renderMemberPicker(el.teamMemberList, { multi: true });
  el.dialogNewTeam.showModal();
});
el.btnNewTeamCancel.addEventListener("click", () => el.dialogNewTeam.close());
el.formNewTeam.addEventListener("submit", async (e) => {
  e.preventDefault();
  const name = el.inputTeamName.value.trim().toLowerCase();
  const description = el.inputTeamDesc.value.trim();
  if (!/^[a-z0-9][a-z0-9_-]{1,31}$/.test(name)) {
    el.newTeamError.textContent = "Invalid team name.";
    el.newTeamError.hidden = false;
    return;
  }
  const selected = [...el.teamMemberList.querySelectorAll("input[type=checkbox]:checked")]
    .map((c) => c.value);
  const members = [...new Set([state.user.uid, ...selected])];
  if (members.length < 2) {
    el.newTeamError.textContent = "Pick at least one other member.";
    el.newTeamError.hidden = false;
    return;
  }
  try {
    const ref = await addDoc(collection(db, "channels"), {
      name,
      description,
      type: "team",
      members,
      createdByUid: state.user.uid,
      createdAt: serverTimestamp(),
      isDefault: false,
      archived: false,
    });
    el.dialogNewTeam.close();
    selectChannel(ref.id);
  } catch (err) {
    el.newTeamError.textContent = err.message;
    el.newTeamError.hidden = false;
  }
});

// ===========================================================================
// New DM dialog
// ===========================================================================

el.btnNewDm.addEventListener("click", () => {
  if (el.newDmError) el.newDmError.hidden = true;
  renderMemberPicker(el.dmMemberList, { multi: true });
  el.dialogNewDm.showModal();
});

// Sidebar DM filter — type to narrow the list by member name.
el.dmFilter?.addEventListener("input", () => {
  state.dmFilter = el.dmFilter.value;
  renderChannelLists();
});
el.btnNewDmCancel.addEventListener("click", () => el.dialogNewDm.close());

el.btnNewDmStart?.addEventListener("click", async () => {
  if (el.newDmError) el.newDmError.hidden = true;
  // The picker auto-checks self (disabled checkbox); exclude that uid.
  const selected = [...el.dmMemberList.querySelectorAll("input[type=checkbox]:checked")]
    .map((c) => c.value)
    .filter((u) => u !== state.user.uid);
  if (selected.length === 0) {
    el.newDmError.textContent = "Pick at least one person.";
    el.newDmError.hidden = false;
    return;
  }
  el.dialogNewDm.close();
  if (selected.length === 1) {
    await openOrCreateDm(selected[0]);
  } else {
    await createGroupDm(selected);
  }
});

// Start a group DM with 2+ other members. Uses a deterministic id so the
// same set of people always reuses the same channel (no accidental duplicate
// "Alice, Bob, Charlie" groups). Auto-unhides if the user previously hid it.
async function createGroupDm(otherUids) {
  const myUid = state.user.uid;
  const members = [...new Set([myUid, ...otherUids])].sort();
  const gdmKey = "gdm_" + members.join("_");
  // Already in my channel cache? Just open + unhide.
  const existing = state.channels.find((c) => c.id === gdmKey);
  if (existing) {
    if (getMyHiddenDms().includes(gdmKey)) await unhideDm(gdmKey);
    selectChannel(gdmKey);
    return;
  }
  try {
    await setDoc(doc(db, "channels", gdmKey), {
      name: "",                     // group DMs have no name; UI derives from members
      description: "",
      type: "dm",
      members,
      createdByUid: myUid,
      createdAt: serverTimestamp(),
      lastMessageAt: serverTimestamp(),
      isDefault: false,
      archived: false,
    }, { merge: true });    // merge so existing members stay if doc already exists
    if (getMyHiddenDms().includes(gdmKey)) await unhideDm(gdmKey);
    selectChannelWhenReady(gdmKey);
  } catch (err) {
    alert("Failed to start group DM: " + err.message);
  }
}

// ===========================================================================
// User profile (view) — opened by clicking any avatar or author name.
// ===========================================================================

function isUserBanned(uidOrUser) {
  const uid = typeof uidOrUser === "string" ? uidOrUser : uidOrUser?.uid;
  if (!uid) return false;
  if ((state.bans.uids || []).includes(uid)) return true;
  const u = typeof uidOrUser === "string" ? getUserByUid(uid) : uidOrUser;
  const email = (u?.email || "").toLowerCase();
  return !!email && (state.bans.emails || []).includes(email);
}

function showUserProfile(uid) {
  if (!uid || !el.dialogUserProfile) return;
  const u = getUserByUid(uid);
  if (!u) {
    // Author left or never had a profile doc — show what we have from message
    // denorm fields if possible, but for the view-only dialog just bail.
    return;
  }
  const banned = isUserBanned(u);
  // Non-admins shouldn't see banned users' profile at all — they were yanked
  // from BRS Chat and shouldn't be re-engageable from message history.
  if (banned && !isAdmin()) return;

  el.userProfileAvatar.innerHTML = "";
  const av = renderAvatar(u, "lg");
  el.userProfileAvatar.appendChild(av);
  el.userProfileName.textContent = u.displayName || "(unnamed)";

  const online = isUserOnline(u);
  if (banned) {
    el.userProfileOnline.textContent = "Banned by admin";
    el.userProfileOnline.className = "user-profile-presence banned";
  } else {
    el.userProfileOnline.textContent = online ? "● Online" : "Offline";
    el.userProfileOnline.className = "user-profile-presence" + (online ? " online" : "");
  }

  if (u.affiliation) {
    el.userProfileAffiliation.textContent = u.affiliation;
    el.userProfileAffiliation.hidden = false;
  } else {
    el.userProfileAffiliation.hidden = true;
  }
  if (u.email) {
    el.userProfileEmail.textContent = u.email;
    el.userProfileEmail.hidden = false;
  } else {
    el.userProfileEmail.hidden = true;
  }

  const isSelf = u.uid === state.user.uid;
  // Send DM hidden for self and for banned users (banned users can't sign in
  // anyway, and adding them as a DM member would silently re-include them).
  el.btnUserProfileDm.hidden = isSelf || banned;
  el.btnUserProfileEdit.hidden = !isSelf;
  // Replace prior listeners (showUserProfile may be called many times).
  el.btnUserProfileDm.onclick = async () => {
    el.dialogUserProfile.close();
    await openOrCreateDm(uid);
  };
  el.btnUserProfileEdit.onclick = () => {
    el.dialogUserProfile.close();
    el.btnEditProfile.click();
  };
  el.dialogUserProfile.showModal();
}

el.btnUserProfileClose?.addEventListener("click", () => el.dialogUserProfile?.close());

async function openOrCreateDm(otherUid) {
  const myUid = state.user.uid;
  // Deterministic DM id so the same 2 people always share the same DM channel.
  const dmKey = "dm_" + [myUid, otherUid].sort().join("_");
  // Does it already exist locally?
  const existing = state.channels.find((c) => c.id === dmKey);
  if (existing) {
    if (getMyHiddenDms().includes(dmKey)) await unhideDm(dmKey);
    selectChannel(dmKey);
    return;
  }
  // Create with a known id.
  try {
    await setDoc(doc(db, "channels", dmKey), {
      name: "dm",
      description: "",
      type: "dm",
      members: [myUid, otherUid].sort(),
      createdByUid: myUid,
      createdAt: serverTimestamp(),
      lastMessageAt: serverTimestamp(),
      isDefault: false,
      archived: false,
    });
    // Give subscribeChannels a moment to deliver the new doc into state.channels;
    // selectChannel bails silently if the id isn't there yet.
    selectChannelWhenReady(dmKey);
  } catch (err) {
    alert("Failed to start DM: " + err.message);
  }
}

// Polls state.channels for the given id and selects it when it appears, with
// a few retries. Used after creating a channel/DM so we don't race the
// onSnapshot delivery.
function selectChannelWhenReady(channelId, attemptsLeft = 10) {
  if (state.channels.find((c) => c.id === channelId)) {
    selectChannel(channelId);
    return;
  }
  if (attemptsLeft <= 0) {
    alert("DM channel was created but didn't appear in the channel list. Try refreshing.");
    return;
  }
  setTimeout(() => selectChannelWhenReady(channelId, attemptsLeft - 1), 150);
}

// Render a list of users with checkboxes (multi) or click-to-pick (single).
function renderMemberPicker(container, { multi, onPick }) {
  container.innerHTML = "";
  const users = state.allUsers
    .filter((u) => u.displayName && !isUserBanned(u))
    .sort((a, b) => (a.displayName || "").localeCompare(b.displayName || ""));

  if (users.length === 0) {
    const p = document.createElement("p");
    p.className = "hint";
    p.textContent = "No other users yet. Ask someone to sign in first.";
    container.appendChild(p);
    return;
  }

  // Filter input — visible when the candidate list is long enough to need it.
  // Rows are hidden with display:none rather than removed, so any checkboxes
  // already ticked in multi-select mode keep their state across filter changes.
  let searchInput = null;
  let emptyHint = null;
  if (users.length >= 5) {
    searchInput = document.createElement("input");
    searchInput.type = "search";
    searchInput.className = "member-picker-search";
    searchInput.placeholder = "Search by name, affiliation, or email…";
    searchInput.autocomplete = "off";
    container.appendChild(searchInput);
  }

  const rows = [];
  for (const u of users) {
    const isSelf = u.uid === state.user.uid;
    const row = document.createElement("label");
    row.className = "member-row" + (isSelf ? " self" : "");
    if (multi) {
      const cb = document.createElement("input");
      cb.type = "checkbox";
      cb.value = u.uid;
      if (isSelf) { cb.disabled = true; cb.checked = true; }
      row.appendChild(cb);
    }
    row.appendChild(renderAvatar(u, "sm"));
    const info = document.createElement("div");
    info.className = "member-info";
    const name = document.createElement("span");
    name.className = "member-name";
    name.textContent = (u.displayName || u.email || "(unknown)") + (isSelf ? " (you)" : "");
    const sub = document.createElement("span");
    sub.className = "member-sub";
    sub.textContent = [u.affiliation, u.email].filter(Boolean).join(" · ");
    info.appendChild(name);
    info.appendChild(sub);
    row.appendChild(info);
    if (!multi && !isSelf && onPick) {
      row.addEventListener("click", () => onPick(u.uid));
    }
    rows.push({ user: u, el: row });
    container.appendChild(row);
  }

  if (searchInput) {
    emptyHint = document.createElement("p");
    emptyHint.className = "hint";
    emptyHint.textContent = "No matching people.";
    emptyHint.style.display = "none";
    container.appendChild(emptyHint);

    searchInput.addEventListener("input", () => {
      const f = searchInput.value.trim().toLowerCase();
      let visibleCount = 0;
      for (const { user, el } of rows) {
        const hay = (
          (user.displayName || "") + " " +
          (user.email || "") + " " +
          (user.affiliation || "")
        ).toLowerCase();
        const match = !f || hay.includes(f);
        el.style.display = match ? "" : "none";
        if (match) visibleCount++;
      }
      emptyHint.style.display = visibleCount === 0 ? "" : "none";
    });

    // Autofocus the search field when the dialog mounts the picker, so the
    // user can start typing immediately.
    setTimeout(() => searchInput.focus(), 50);
  }
}

// ===========================================================================
// Channel membership + #general auto-join
// ===========================================================================

const GENERAL_CHANNEL_NAME = "general";
let generalJoinAttempted = false;

function findGeneralChannel() {
  return state.channels.find(
    (c) => (c.name || "").toLowerCase() === GENERAL_CHANNEL_NAME && c.type !== "dm",
  );
}

// Independent of the members-filtered channel subscription: query the
// "general" channel directly and add self to its members. This is how
// brand-new users get bootstrapped into #general (they can't see it via
// the subscription because they're not yet a member).
async function ensureJoinedGeneral() {
  if (generalJoinAttempted) return;
  // Banned users must not be auto-rejoined.
  if (isBanned()) {
    generalJoinAttempted = true;
    return;
  }
  generalJoinAttempted = true;
  try {
    const snap = await getDocs(query(
      collection(db, "channels"),
      where("name", "==", GENERAL_CHANNEL_NAME),
    ));
    const d = snap.docs[0];
    if (!d) {
      console.warn("No #general channel found in Firestore — seed one with name='general'.");
      generalJoinAttempted = false;
      return;
    }
    const members = d.data().members || [];
    if (!members.includes(state.user.uid)) {
      await updateDoc(doc(db, "channels", d.id), {
        members: arrayUnion(state.user.uid),
      });
    }
  } catch (err) {
    console.warn("failed to auto-join #general", err);
    generalJoinAttempted = false;  // allow retry next cycle
  }
}

el.btnMembers.addEventListener("click", () => {
  if (!state.currentChannelId) return;
  renderMembersDialog();
  el.dialogMembers.showModal();
});
el.btnMembersClose.addEventListener("click", () => el.dialogMembers.close());

function renderMembersDialog() {
  const ch = state.channels.find((c) => c.id === state.currentChannelId);
  if (!ch) return;
  const isGeneral = (ch.name || "").toLowerCase() === GENERAL_CHANNEL_NAME;
  const isGroupDm = ch.type === "dm" && (ch.members || []).length > 2;
  let prefix, title;
  if (isGroupDm) {
    prefix = "👥 ";
    title = `Members of ${prefix}${dmLabel(ch)}`;
  } else {
    prefix = ch.type === "team" ? "🔒 " : "#";
    title = `Members of ${prefix}${ch.name}`;
  }
  const admin = isAdmin();

  el.membersTitle.textContent = title;
  el.membersHint.textContent = isGroupDm
    ? "Add or remove members from this group DM. Anyone with chat access can be invited."
    : isGeneral
      ? (admin
          ? "#general includes everyone automatically. As an admin you can force-remove anyone (banning)."
          : "#general includes everyone automatically. Only admins can remove members.")
      : (admin
          ? "You can add anyone and remove anyone (admin)."
          : "You can invite anyone. You can only leave yourself — only admins can remove others.");

  const memberUids = new Set(ch.members || []);
  const memberUsers = state.allUsers.filter((u) => memberUids.has(u.uid));
  const nonMembers = state.allUsers
    .filter((u) => !memberUids.has(u.uid) && !isUserBanned(u));

  // Current members
  el.membersList.innerHTML = "";
  if (memberUsers.length === 0) {
    const p = document.createElement("p");
    p.className = "hint";
    p.textContent = "Nobody yet.";
    el.membersList.appendChild(p);
  }
  for (const u of memberUsers) {
    const isSelf = u.uid === state.user.uid;
    const row = document.createElement("div");
    row.className = "member-row";
    row.appendChild(renderAvatar(u, "sm"));
    const info = document.createElement("div");
    info.className = "member-info";
    const name = document.createElement("span");
    name.className = "member-name";
    const adminBadge = state.adminEmails.includes((u.email || "").toLowerCase()) ? " 🛡️" : "";
    name.textContent = u.displayName + (isSelf ? " (you)" : "") + adminBadge;
    const sub = document.createElement("span");
    sub.className = "member-sub";
    sub.textContent = [u.affiliation, u.email].filter(Boolean).join(" · ");
    info.appendChild(name);
    info.appendChild(sub);
    row.appendChild(info);

    // Remove / Leave rules:
    //  - #general: only admins can force-remove (including self, though self
    //    will be re-added on next sign-in). Non-admins get no button.
    //  - Other channels: self can leave; admins can remove anyone.
    const canAct = isGeneral ? admin : (isSelf || admin);
    if (canAct) {
      const rm = document.createElement("button");
      rm.type = "button";
      rm.className = "remove-btn";
      rm.textContent = isSelf && !isGeneral ? "Leave" : "Remove";
      rm.title = isGeneral
        ? "Force-remove from #general (admin)"
        : (isSelf ? "Leave this channel" : "Remove from channel (admin)");
      rm.addEventListener("click", () => removeMember(ch.id, u.uid));
      row.appendChild(rm);
    }
    // Ban: admin only, never self. Bans the user from the entire chat.
    if (admin && !isSelf) {
      const ban = document.createElement("button");
      ban.type = "button";
      ban.className = "remove-btn ban-btn";
      ban.textContent = "Ban";
      ban.title = "Ban from BRS Chat (admin) — removes from all channels and blocks re-sign-in";
      ban.addEventListener("click", () => banUser(u.uid, u.email, u.displayName));
      row.appendChild(ban);
    }
    el.membersList.appendChild(row);
  }

  // Add someone
  el.addMemberList.innerHTML = "";
  if (nonMembers.length === 0) {
    const p = document.createElement("p");
    p.className = "hint";
    p.textContent = "Everyone is already here.";
    el.addMemberList.appendChild(p);
  }
  for (const u of nonMembers) {
    const row = document.createElement("div");
    row.className = "member-row";
    row.appendChild(renderAvatar(u, "sm"));
    const info = document.createElement("div");
    info.className = "member-info";
    const name = document.createElement("span");
    name.className = "member-name";
    name.textContent = u.displayName;
    const sub = document.createElement("span");
    sub.className = "member-sub";
    sub.textContent = [u.affiliation, u.email].filter(Boolean).join(" · ");
    info.appendChild(name);
    info.appendChild(sub);
    row.appendChild(info);
    const add = document.createElement("button");
    add.type = "button";
    add.className = "add-btn";
    add.textContent = "Add";
    add.addEventListener("click", () => addMember(ch.id, u.uid));
    row.appendChild(add);
    el.addMemberList.appendChild(row);
  }
}

async function addMember(channelId, uid) {
  try {
    await updateDoc(doc(db, "channels", channelId), {
      members: arrayUnion(uid),
    });
  } catch (err) {
    alert("Failed to add: " + err.message);
  }
}
async function removeMember(channelId, uid) {
  const isSelf = uid === state.user.uid;
  const msg = isSelf ? "Leave this channel?" : "Remove this member from the channel?";
  if (!window.confirm(msg)) return;
  try {
    await updateDoc(doc(db, "channels", channelId), {
      members: arrayRemove(uid),
    });
  } catch (err) {
    alert("Failed: " + err.message);
  }
}

// Ban: yank a user from every channel and prevent re-sign-in.
// Past messages are preserved (delete them separately if needed).
// Reversible via Unban from the Admin panel.
async function banUser(uid, email, displayName) {
  if (!isAdmin()) return;
  if (uid === state.user.uid) {
    alert("You can't ban yourself.");
    return;
  }
  const reason = window.prompt(
    `Ban "${displayName}" from BRS Chat?\n\n` +
    `This will:\n` +
    `  • Remove them from every channel (public, team, and DM)\n` +
    `  • Sign them out and block them from signing back in\n` +
    `  • Past messages remain (delete those individually if needed)\n\n` +
    `This is reversible — you can Unban them later from Admin panel.\n\n` +
    `Reason (optional, recorded for audit):`,
    "",
  );
  if (reason === null) return;  // cancelled
  try {
    // Find every channel the user is a member of and remove them in parallel.
    const snap = await getDocs(query(
      collection(db, "channels"),
      where("members", "array-contains", uid),
    ));
    await Promise.all(snap.docs.map((d) =>
      updateDoc(d.ref, { members: arrayRemove(uid) })
    ));
    // Add to bans list (uid for known users; email for future reuse / Phase A).
    const updates = {
      uids: arrayUnion(uid),
      updatedAt: serverTimestamp(),
      updatedBy: state.user.uid,
      lastReason: reason || null,
    };
    if (email) updates.emails = arrayUnion((email || "").toLowerCase());
    await setDoc(doc(db, "config", "bans"), updates, { merge: true });
    alert(`${displayName} has been banned. They were removed from ${snap.size} channel(s).`);
  } catch (err) {
    alert("Ban failed: " + err.message);
  }
}

// Unban: removes user from config/bans. They can sign in again and will
// auto-rejoin #general on next sign-in. Other channels need re-invite.
async function unbanUser(uid, email, displayName) {
  if (!isAdmin()) return;
  if (!window.confirm(
    `Unban ${displayName}?\n\n` +
    `They'll be able to sign in again and will auto-rejoin #general on next sign-in. ` +
    `Other channels need to be invited manually.`
  )) return;
  try {
    const updates = {
      updatedAt: serverTimestamp(),
      updatedBy: state.user.uid,
    };
    if (uid) updates.uids = arrayRemove(uid);
    if (email) updates.emails = arrayRemove((email || "").toLowerCase());
    await setDoc(doc(db, "config", "bans"), updates, { merge: true });
  } catch (err) {
    alert("Unban failed: " + err.message);
  }
}

// ---- Admin panel ----

el.btnAdminPanel?.addEventListener("click", () => {
  el.userMenuDropdown.hidden = true;
  el.btnUserMenu.setAttribute("aria-expanded", "false");
  renderAdminDialog();
  el.dialogAdmin.showModal();
});
el.btnAdminClose?.addEventListener("click", () => el.dialogAdmin.close());

function renderAdminDialog() {
  el.adminError.hidden = true;
  el.inputNewAdmin.value = "";
  el.adminList.innerHTML = "";
  if (state.adminEmails.length === 0) {
    const p = document.createElement("p");
    p.className = "hint";
    p.textContent = "No admins set. Add one below.";
    el.adminList.appendChild(p);
  } else {
    for (const email of state.adminEmails) {
      const row = document.createElement("div");
      row.className = "member-row";
      row.style.padding = "4px 0";
      const label = document.createElement("span");
      label.style.flex = "1";
      label.textContent = email + (email === (state.user.email || "").toLowerCase() ? " (you)" : "");
      row.appendChild(label);
      if (isAdmin()) {
        const rm = document.createElement("button");
        rm.type = "button";
        rm.className = "remove-btn";
        rm.textContent = "Revoke";
        rm.addEventListener("click", () => removeAdmin(email));
        row.appendChild(rm);
      }
      el.adminList.appendChild(row);
    }
  }
  renderBansList();
  renderMaintenanceControls();
}

function renderBansList() {
  if (!el.bansList) return;
  el.bansList.innerHTML = "";
  // Build a unified list keyed by uid OR email (some entries may have only one).
  const rows = [];
  for (const uid of (state.bans.uids || [])) {
    const u = state.allUsers.find((x) => x.uid === uid);
    rows.push({
      uid,
      email: (u?.email || "").toLowerCase(),
      displayName: u?.displayName || `(unknown user · uid ${uid.slice(0, 6)}…)`,
    });
  }
  for (const email of (state.bans.emails || [])) {
    if (rows.find((r) => r.email === email)) continue;  // already covered by uid row
    rows.push({ uid: null, email, displayName: email });
  }
  if (rows.length === 0) {
    const p = document.createElement("p");
    p.className = "hint";
    p.textContent = "No one is banned.";
    el.bansList.appendChild(p);
    return;
  }
  for (const r of rows) {
    const row = document.createElement("div");
    row.className = "member-row";
    row.style.padding = "4px 0";
    const label = document.createElement("span");
    label.style.flex = "1";
    label.textContent = r.displayName + (r.email && r.displayName !== r.email ? ` <${r.email}>` : "");
    row.appendChild(label);
    if (isAdmin()) {
      const ub = document.createElement("button");
      ub.type = "button";
      ub.className = "secondary-btn";
      ub.style.padding = "4px 12px";
      ub.style.width = "auto";
      ub.textContent = "Unban";
      ub.addEventListener("click", () => unbanUser(r.uid, r.email, r.displayName));
      row.appendChild(ub);
    }
    el.bansList.appendChild(row);
  }
}

el.btnAddAdmin?.addEventListener("click", async () => {
  el.adminError.hidden = true;
  const email = (el.inputNewAdmin.value || "").trim().toLowerCase();
  if (!email || !email.includes("@")) {
    el.adminError.textContent = "Please enter a valid email.";
    el.adminError.hidden = false;
    return;
  }
  if (!isAdmin()) {
    el.adminError.textContent = "Only existing admins can grant admin rights.";
    el.adminError.hidden = false;
    return;
  }
  try {
    await setDoc(doc(db, "config", "admins"), {
      emails: arrayUnion(email),
    }, { merge: true });
    el.inputNewAdmin.value = "";
  } catch (err) {
    el.adminError.textContent = err.message;
    el.adminError.hidden = false;
  }
});

async function removeAdmin(email) {
  if (!isAdmin()) return;
  if (email === (state.user.email || "").toLowerCase() &&
      !window.confirm("This will revoke YOUR admin rights. Continue?")) return;
  try {
    await setDoc(doc(db, "config", "admins"), {
      emails: arrayRemove(email),
    }, { merge: true });
  } catch (err) {
    el.adminError.textContent = err.message;
    el.adminError.hidden = false;
  }
}

// ===========================================================================
// Browse public channels
// ===========================================================================

el.btnBrowseChannels?.addEventListener("click", () => {
  renderBrowseChannels();
  el.dialogBrowseChannels.showModal();
});
el.btnBrowseChannelsClose?.addEventListener("click", () => el.dialogBrowseChannels.close());

async function renderBrowseChannels() {
  el.browseChannelsList.innerHTML = "Loading…";
  try {
    const snap = await getDocs(query(collection(db, "channels"), where("type", "==", "public")));
    const mine = new Set(state.channels.map((c) => c.id));
    const others = snap.docs
      .map((d) => ({ id: d.id, ...d.data() }))
      .filter((c) => !c.archived && !mine.has(c.id));
    el.browseChannelsList.innerHTML = "";
    if (others.length === 0) {
      el.browseChannelsList.innerHTML = `<p class="hint">You're already in every public channel.</p>`;
      return;
    }
    for (const c of others) {
      const item = document.createElement("div");
      item.className = "bookmark-item";
      const info = document.createElement("div");
      info.className = "bookmark-item-body";
      const name = document.createElement("div");
      name.innerHTML = `<strong>#${escHtml(c.name)}</strong>`;
      const sub = document.createElement("div");
      sub.className = "bookmark-item-meta";
      sub.textContent = (c.description || "") +
        (c.description ? " · " : "") +
        `${(c.members || []).length} members`;
      info.appendChild(name);
      info.appendChild(sub);
      item.appendChild(info);
      const joinBtn = document.createElement("button");
      joinBtn.type = "button";
      joinBtn.className = "primary-btn";
      joinBtn.style.width = "auto";
      joinBtn.style.padding = "6px 14px";
      joinBtn.textContent = "Join";
      joinBtn.addEventListener("click", async () => {
        joinBtn.disabled = true;
        try {
          await updateDoc(doc(db, "channels", c.id), {
            members: arrayUnion(state.user.uid),
          });
          el.dialogBrowseChannels.close();
          // The subscription will pick it up; select it shortly.
          setTimeout(() => selectChannel(c.id), 200);
        } catch (err) {
          alert("Join failed: " + err.message);
          joinBtn.disabled = false;
        }
      });
      item.appendChild(joinBtn);
      el.browseChannelsList.appendChild(item);
    }
  } catch (err) {
    el.browseChannelsList.innerHTML = `<p class="error">${escHtml(err.message)}</p>`;
  }
}

// ===========================================================================
// Sidebar toggle (mobile)
// ===========================================================================

function openMobileSidebar() {
  el.chatSidebar.classList.add("open");
  if (el.sidebarBackdrop) el.sidebarBackdrop.hidden = false;
}
function closeMobileSidebar() {
  el.chatSidebar.classList.remove("open");
  if (el.sidebarBackdrop) el.sidebarBackdrop.hidden = true;
}
el.btnToggleSidebar.addEventListener("click", () => {
  if (el.chatSidebar.classList.contains("open")) closeMobileSidebar();
  else openMobileSidebar();
});
el.sidebarBackdrop?.addEventListener("click", closeMobileSidebar);

// ===========================================================================
// Delete (archive) channel — creator + admin. Hides from sidebar, name can
// be reused. Messages remain in Firestore for audit / future undelete.
// ===========================================================================

el.btnDeleteChannel?.addEventListener("click", async () => {
  const cid = state.currentChannelId;
  const ch = state.channels.find((c) => c.id === cid);
  if (!ch) return;
  const label = ch.type === "team" ? `🔒${ch.name}` : `#${ch.name}`;
  const ok = confirm(
    `Delete ${label}?\n\n` +
    `• The channel disappears from everyone's sidebar.\n` +
    `• Messages are not erased.\n` +
    `• The name becomes free to reuse for a new channel.`
  );
  if (!ok) return;
  try {
    await updateDoc(doc(db, "channels", cid), { archived: true });
    const fallback = state.channels.find((c) => c.id !== cid && !c.archived);
    if (fallback) selectChannel(fallback.id);
  } catch (err) {
    alert("Delete failed: " + err.message);
  }
});

// ===========================================================================
// Mobile keyboard / visualViewport
// ===========================================================================

if (window.visualViewport) {
  window.visualViewport.addEventListener("resize", () => {
    // Ensure message list stays scrolled to bottom when keyboard opens.
    if (isScrolledToBottom(el.messagesContainer)) {
      scrollToBottom(el.messagesContainer);
    }
  });
}

// ===========================================================================
// #1 Pinned messages
// ===========================================================================

function renderPinnedBar(docs, channelId) {
  const pinned = docs.filter((m) => m.pinned && !m.deleted);
  el.pinnedCount.textContent = pinned.length;
  el.pinnedBar.hidden = pinned.length === 0;
  el.pinnedList.innerHTML = "";
  for (const m of pinned) {
    const li = document.createElement("li");
    li.className = "pinned-item";
    li.appendChild(renderAvatar(getUserByUid(m.authorUid) || { authorName: m.authorName }, "xs"));
    const text = document.createElement("div");
    text.className = "pinned-item-text";
    text.innerHTML =
      `<span class="pinned-item-author"></span> ` +
      `<span class="pinned-item-preview"></span>`;
    text.querySelector(".pinned-item-author").textContent =
      (getUserByUid(m.authorUid)?.displayName || m.authorName || "?") + ":";
    const preview = (m.text || (m.image ? "[image]" : "[poll]")).slice(0, 120);
    text.querySelector(".pinned-item-preview").textContent = preview;
    li.appendChild(text);
    li.addEventListener("click", () => scrollToMessage(m.id));
    el.pinnedList.appendChild(li);
  }
}

el.btnPinnedToggle.addEventListener("click", () => {
  const open = !el.pinnedList.hidden;
  el.pinnedList.hidden = open;
  el.pinnedBar.classList.toggle("open", !open);
});

async function togglePin(channelId, m) {
  try {
    await updateDoc(msgDocRef(channelId, m), {
      pinned: !m.pinned,
    });
  } catch (err) {
    alert("Pin failed: " + err.message);
  }
}

// ===========================================================================
// #2 Threaded replies
// ===========================================================================

async function openThread(channelId, parentMsgId) {
  closeThread();
  state.threadMsgId = parentMsgId;
  el.threadPanel.hidden = false;
  el.threadParent.innerHTML = "Loading…";
  el.threadReplies.innerHTML = "";
  // Fetch parent
  try {
    const snap = await getDoc(doc(db, "channels", channelId, "messages", parentMsgId));
    if (!snap.exists()) { el.threadParent.textContent = "(Message not found)"; return; }
    state.threadParent = { id: snap.id, ...snap.data() };
    if (isHiddenForViewer(state.threadParent)) {
      el.threadParent.textContent = "(Message not found)";
      return;
    }
    el.threadParent.innerHTML = "";
    el.threadParent.appendChild(renderMessage(state.threadParent, channelId));
  } catch (err) {
    el.threadParent.textContent = "Failed to load: " + err.message;
    return;
  }
  // Subscribe to replies
  const q = query(
    collection(db, "channels", channelId, "messages", parentMsgId, "replies"),
    orderBy("createdAt", "asc"),
  );
  state.unsubThreadReplies = onSnapshot(q, (snap) => {
    el.threadReplies.innerHTML = "";
    for (const d of snap.docs) {
      const m = { id: d.id, ...d.data() };
      if (isHiddenForViewer(m)) continue;
      el.threadReplies.appendChild(renderMessage(m, channelId));
    }
    el.threadReplies.scrollTop = el.threadReplies.scrollHeight;
  });
}
function closeThread() {
  if (state.unsubThreadReplies) { state.unsubThreadReplies(); state.unsubThreadReplies = null; }
  state.threadMsgId = null;
  state.threadParent = null;
  el.threadPanel.hidden = true;
}
el.btnThreadClose.addEventListener("click", closeThread);

el.formThreadCompose.addEventListener("submit", async (e) => {
  e.preventDefault();
  const text = el.inputThreadMessage.value.trim();
  if (!text) return;
  const channelId = state.currentChannelId;
  const parentId = state.threadMsgId;
  if (!channelId || !parentId) return;
  const { mentionUids, mentionsEveryone } = parseMentions(text);
  el.inputThreadMessage.value = "";
  try {
    await addDoc(collection(db, "channels", channelId, "messages", parentId, "replies"), {
      text,
      authorUid: state.user.uid,
      authorEmail: state.user.email,
      authorName: state.userDoc.displayName,
      authorAffiliation: state.userDoc.affiliation || "",
      mentions: mentionUids,
      mentionsEveryone,
      createdAt: serverTimestamp(),
      parentId,
    });
    // Update parent with replyCount + a denormalized lastReply so that the
    // channel-level notify listener picks up thread replies for webhook
    // forwarding and browser notifications.
    updateDoc(doc(db, "channels", channelId, "messages", parentId), {
      replyCount: increment(1),
      lastReplyAt: serverTimestamp(),
      lastReply: {
        text,
        authorUid: state.user.uid,
        authorEmail: state.user.email,
        authorName: state.userDoc.displayName,
        mentions: mentionUids,
        mentionsEveryone,
      },
    }).then(() => {
      console.log("[reply] parent updated with lastReply", { parentId, mentions: mentionUids });
    }).catch((err) => console.warn("reply parent update failed", err));
  } catch (err) {
    alert("Reply failed: " + err.message);
  }
});

// ===========================================================================
// #3 Search (incremental, current channel)
// ===========================================================================

function openSearch() {
  el.searchBar.hidden = false;
  el.inputSearch.focus();
  el.inputSearch.select();
}
function closeSearch() {
  el.searchBar.hidden = true;
  if (state.searchQuery) {
    state.searchQuery = "";
    el.searchCount.textContent = "";
    renderMessages(state.currentMessages, state.currentChannelId);
  }
}
el.btnSearch.addEventListener("click", openSearch);
el.btnSearchClose.addEventListener("click", closeSearch);
el.inputSearch.addEventListener("input", () => {
  state.searchQuery = el.inputSearch.value;
  renderMessages(state.currentMessages, state.currentChannelId);
});
el.inputSearch.addEventListener("keydown", (e) => {
  if (e.key === "Escape") { e.preventDefault(); closeSearch(); }
});

// ===========================================================================
// #5 Polls
// ===========================================================================

el.btnPoll.addEventListener("click", openPollDialog);
function openPollDialog() {
  el.pollQuestion.value = "";
  el.pollOptions.value = "";
  el.pollMulti.checked = false;
  el.pollError.hidden = true;
  el.dialogPoll.showModal();
}
el.btnPollCancel.addEventListener("click", () => el.dialogPoll.close());
el.formPoll.addEventListener("submit", async (e) => {
  e.preventDefault();
  const question = el.pollQuestion.value.trim();
  const options = el.pollOptions.value.split(/\r?\n/).map((s) => s.trim()).filter(Boolean);
  const multi = el.pollMulti.checked;
  if (!question) { el.pollError.textContent = "Question required."; el.pollError.hidden = false; return; }
  if (options.length < 2) { el.pollError.textContent = "Need at least 2 options."; el.pollError.hidden = false; return; }
  if (options.length > 10) { el.pollError.textContent = "Maximum 10 options."; el.pollError.hidden = false; return; }
  const channelId = state.currentChannelId;
  try {
    await addDoc(collection(db, "channels", channelId, "messages"), {
      type: "poll",
      text: "",
      image: null,
      poll: {
        question,
        options: options.map((label) => ({ label, votes: [] })),
        multi,
      },
      authorUid: state.user.uid,
      authorEmail: state.user.email,
      authorName: state.userDoc.displayName,
      authorAffiliation: state.userDoc.affiliation || "",
      mentions: [],
      mentionsEveryone: false,
      createdAt: serverTimestamp(),
      editedAt: null,
      deleted: false,
      backedUpAt: null,
    });
    updateDoc(doc(db, "channels", channelId), {
      lastMessageAt: serverTimestamp(),
    }).catch((e) => console.warn("lastMessageAt bump failed", e));
    el.dialogPoll.close();
  } catch (err) {
    el.pollError.textContent = err.message;
    el.pollError.hidden = false;
  }
});

function renderPollCard(channelId, m) {
  const { question, options = [], multi } = m.poll || {};
  const card = document.createElement("div");
  card.className = "poll-card";
  const q = document.createElement("div");
  q.className = "poll-question";
  q.innerHTML = icon("bar-chart-3", { size: 16 }) + " " + escHtml(question);
  card.appendChild(q);
  const total = options.reduce((sum, o) => sum + (o.votes || []).length, 0);
  options.forEach((opt, idx) => {
    const uids = opt.votes || [];
    const voted = uids.includes(state.user.uid);
    const btn = document.createElement("button");
    btn.type = "button";
    btn.className = "poll-option" + (voted ? " voted" : "");
    const pct = total > 0 ? Math.round((uids.length / total) * 100) : 0;
    btn.innerHTML =
      `<span class="poll-bar" style="width:${pct}%"></span>` +
      `<span><span class="poll-label"></span>` +
      `<span class="poll-count">${uids.length} · ${pct}%</span></span>`;
    btn.querySelector(".poll-label").textContent = opt.label;
    const voterNames = () => uids
      .map((u) => getUserByUid(u)?.displayName || "")
      .filter(Boolean)
      .join(", ");
    btn.title = voterNames();
    attachLongPressTooltip(btn, voterNames);
    btn.addEventListener("click", () => togglePollVote(channelId, m, idx));
    card.appendChild(btn);
  });
  const totalEl = document.createElement("div");
  totalEl.className = "poll-total";
  totalEl.textContent = `${total} vote${total === 1 ? "" : "s"}${multi ? " · multi-select" : ""}`;
  card.appendChild(totalEl);
  return card;
}

async function togglePollVote(channelId, m, optionIdx) {
  const uid = state.user.uid;
  const poll = JSON.parse(JSON.stringify(m.poll));
  const isVoted = (poll.options[optionIdx].votes || []).includes(uid);
  if (!poll.multi) {
    // Single-choice: remove uid from all options first
    for (const o of poll.options) { o.votes = (o.votes || []).filter((u) => u !== uid); }
  }
  if (isVoted) {
    poll.options[optionIdx].votes = (poll.options[optionIdx].votes || []).filter((u) => u !== uid);
  } else {
    poll.options[optionIdx].votes = [...(poll.options[optionIdx].votes || []), uid];
  }
  try {
    await updateDoc(doc(db, "channels", channelId, "messages", m.id), { poll });
  } catch (err) {
    console.error(err);
  }
}

// ===========================================================================
// #6 Online presence
// ===========================================================================

function startHeartbeat() {
  stopHeartbeat();
  const beat = async () => {
    if (!document.hasFocus()) return;
    try {
      await updateDoc(doc(db, "users", state.user.uid), { lastSeenAt: serverTimestamp() });
    } catch (_) { /* ignore */ }
  };
  beat();
  state.heartbeatTimer = setInterval(beat, 30000);
  window.addEventListener("focus", beat);
}
function stopHeartbeat() {
  if (state.heartbeatTimer) { clearInterval(state.heartbeatTimer); state.heartbeatTimer = null; }
}
function isUserOnline(user) {
  const ts = user?.lastSeenAt;
  if (!ts) return false;
  const ms = ts.toMillis ? ts.toMillis() : (ts.seconds ? ts.seconds * 1000 : 0);
  return Date.now() - ms < 90 * 1000;
}

// ===========================================================================
// #7 Typing indicator
// ===========================================================================

function onComposeTyping() {
  if (!state.currentChannelId) return;
  const ch = state.channels.find((c) => c.id === state.currentChannelId);
  if (!ch || ch.type === "dm") { /* ok, still show */ }
  clearTimeout(state.typingWriteTimer);
  state.typingWriteTimer = setTimeout(() => {
    setDoc(doc(db, "channels", state.currentChannelId, "typing", state.user.uid), {
      name: state.userDoc?.displayName || "",
      ts: serverTimestamp(),
    }).catch(() => {});
  }, 300);
}
function subscribeTypingForCurrent() {
  if (state.unsubTyping) { state.unsubTyping(); state.unsubTyping = null; }
  if (!state.currentChannelId) return;
  const q = collection(db, "channels", state.currentChannelId, "typing");
  state.unsubTyping = onSnapshot(q, (snap) => {
    const now = Date.now();
    const typers = [];
    snap.docs.forEach((d) => {
      if (d.id === state.user.uid) return;
      const data = d.data();
      const ms = data.ts?.toMillis ? data.ts.toMillis() : 0;
      if (now - ms < 5000) typers.push(data.name || "Someone");
    });
    if (typers.length === 0) { el.typingIndicator.hidden = true; el.typingIndicator.textContent = ""; return; }
    el.typingIndicator.hidden = false;
    el.typingIndicator.textContent = typers.length === 1
      ? `${typers[0]} is typing…`
      : `${typers.slice(0, 2).join(", ")}${typers.length > 2 ? ` +${typers.length - 2}` : ""} are typing…`;
  });
}
async function clearOwnTyping() {
  if (!state.currentChannelId) return;
  deleteDoc(doc(db, "channels", state.currentChannelId, "typing", state.user.uid)).catch(() => {});
}

// ===========================================================================
// #9 Bookmarks / Saved messages
// ===========================================================================

async function toggleSave(channelId, m) {
  const saved = state.userDoc?.savedMessages || [];
  const key = (s) => s.channelId + "/" + s.messageId;
  const mine = { channelId, messageId: m.id };
  const isSaved = saved.some((s) => key(s) === key(mine));
  const next = isSaved
    ? saved.filter((s) => key(s) !== key(mine))
    : [...saved, { channelId, messageId: m.id, savedAt: new Date().toISOString() }];
  try {
    await updateDoc(doc(db, "users", state.user.uid), { savedMessages: next });
    state.userDoc.savedMessages = next;
    // Re-render current list to update icon
    renderMessages(state.currentMessages, state.currentChannelId);
  } catch (err) {
    alert("Save failed: " + err.message);
  }
}

el.btnShowBookmarks.addEventListener("click", async () => {
  el.userMenuDropdown.hidden = true;
  el.btnUserMenu.setAttribute("aria-expanded", "false");
  await openBookmarks();
});
el.btnBookmarksClose.addEventListener("click", () => el.dialogBookmarks.close());

async function openBookmarks() {
  el.bookmarksList.innerHTML = "Loading…";
  el.dialogBookmarks.showModal();
  const saved = (state.userDoc?.savedMessages || []).slice().reverse();
  if (saved.length === 0) {
    el.bookmarksList.innerHTML = `<p class="hint">You haven't saved anything yet. Click 🔖 on a message to save it.</p>`;
    return;
  }
  el.bookmarksList.innerHTML = "";
  for (const s of saved) {
    try {
      const snap = await getDoc(doc(db, "channels", s.channelId, "messages", s.messageId));
      if (!snap.exists()) continue;
      const m = { id: snap.id, ...snap.data() };
      const ch = state.channels.find((c) => c.id === s.channelId);
      const item = document.createElement("div");
      item.className = "bookmark-item";
      item.appendChild(renderAvatar(getUserByUid(m.authorUid) || { authorName: m.authorName }, "sm"));
      const bodyEl = document.createElement("div");
      bodyEl.className = "bookmark-item-body";
      const meta = document.createElement("div");
      meta.className = "bookmark-item-meta";
      meta.textContent =
        (ch ? (ch.type === "dm" ? "@" + dmLabel(ch) : "#" + ch.name) : "(channel)") +
        " · " + (getUserByUid(m.authorUid)?.displayName || m.authorName || "") + " · " + formatTime(m.createdAt);
      const text = document.createElement("div");
      text.innerHTML = renderMessageBody(m.text || (m.image ? "[image]" : "[message]")).html;
      bodyEl.appendChild(meta);
      bodyEl.appendChild(text);
      item.appendChild(bodyEl);
      item.addEventListener("click", () => {
        el.dialogBookmarks.close();
        selectChannel(s.channelId);
        state.pendingScrollToMsg = s.messageId;
      });
      el.bookmarksList.appendChild(item);
    } catch (err) {
      console.warn("bookmark load failed", err);
    }
  }
}

// ===========================================================================
// #10 Permalinks
// ===========================================================================

function channelPermalink(channelId, messageId) {
  const base = window.location.origin + window.location.pathname;
  return `${base}#c=${encodeURIComponent(channelId)}&m=${encodeURIComponent(messageId)}`;
}
async function copyPermalink(channelId, messageId) {
  const url = channelPermalink(channelId, messageId);
  try {
    await navigator.clipboard.writeText(url);
    showFlashMessage("Link copied to clipboard");
  } catch (_) {
    prompt("Copy this link:", url);
  }
}
function showFlashMessage(text) {
  const div = document.createElement("div");
  div.className = "fatal-error";
  div.style.background = "var(--surface)";
  div.style.color = "var(--text-color)";
  div.style.borderColor = "var(--accent)";
  div.textContent = text;
  document.body.appendChild(div);
  setTimeout(() => div.remove(), 1800);
}

function handlePermalinkInHash() {
  const hash = window.location.hash.replace(/^#/, "");
  if (!hash) return;
  const params = new URLSearchParams(hash);
  const c = params.get("c");
  const m = params.get("m");
  if (c) {
    // Channel list may not be loaded yet; defer until it is.
    const trySelect = () => {
      if (state.channels.find((ch) => ch.id === c)) {
        selectChannel(c);
        if (m) state.pendingScrollToMsg = m;
        return true;
      }
      return false;
    };
    if (!trySelect()) {
      const interval = setInterval(() => {
        if (trySelect()) clearInterval(interval);
      }, 200);
      setTimeout(() => clearInterval(interval), 5000);
    }
  }
  // Clean the hash so subsequent clicks work.
  history.replaceState({}, "", window.location.pathname);
}

// ===========================================================================
// #11 Keyboard shortcuts + Channel switcher
// ===========================================================================

document.addEventListener("keydown", (e) => {
  // Don't fire shortcuts when typing in inputs
  const tag = (e.target?.tagName || "").toLowerCase();
  const inInput = tag === "input" || tag === "textarea" || e.target?.isContentEditable;
  if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === "k") {
    e.preventDefault();
    openSwitcher();
    return;
  }
  if (e.key === "Escape") {
    if (el.dialogSwitcher.open) el.dialogSwitcher.close();
    if (!el.searchBar.hidden) { closeSearch(); return; }
    if (!el.threadPanel.hidden) { closeThread(); return; }
  }
  if (!inInput && e.key === "/") {
    if (el.chatScreen.hidden) return;
    e.preventDefault();
    openSearch();
  }
});

function openSwitcher() {
  el.inputSwitcher.value = "";
  renderSwitcherResults("");
  el.dialogSwitcher.showModal();
  setTimeout(() => el.inputSwitcher.focus(), 0);
}
el.inputSwitcher.addEventListener("input", () => renderSwitcherResults(el.inputSwitcher.value));
el.inputSwitcher.addEventListener("keydown", (e) => {
  const items = [...el.switcherResults.querySelectorAll(".switcher-result")];
  const idx = items.findIndex((x) => x.classList.contains("active"));
  if (e.key === "ArrowDown") {
    e.preventDefault();
    if (items.length === 0) return;
    items.forEach((x) => x.classList.remove("active"));
    items[Math.min(items.length - 1, idx + 1)].classList.add("active");
    items[Math.min(items.length - 1, idx + 1)].scrollIntoView({ block: "nearest" });
  } else if (e.key === "ArrowUp") {
    e.preventDefault();
    if (items.length === 0) return;
    items.forEach((x) => x.classList.remove("active"));
    items[Math.max(0, idx - 1)].classList.add("active");
    items[Math.max(0, idx - 1)].scrollIntoView({ block: "nearest" });
  } else if (e.key === "Enter") {
    e.preventDefault();
    const act = items.find((x) => x.classList.contains("active")) || items[0];
    if (act) act.click();
  }
});
function renderSwitcherResults(q) {
  el.switcherResults.innerHTML = "";
  const qlow = q.trim().toLowerCase();
  // Channels
  const chs = state.channels.filter((c) => !c.archived).filter((c) => {
    const label = c.type === "dm" ? dmLabel(c) : c.name;
    return !qlow || label.toLowerCase().includes(qlow);
  }).slice(0, 20);
  // Users (for DM jump)
  const us = state.allUsers.filter((u) => u.uid !== state.user.uid && u.displayName)
    .filter((u) => !qlow || (u.displayName + " " + (u.email || "")).toLowerCase().includes(qlow))
    .slice(0, 10);
  let first = true;
  for (const c of chs) {
    const row = document.createElement("div");
    row.className = "switcher-result" + (first ? " active" : "");
    first = false;
    const isDm = c.type === "dm";
    const prefix = isDm ? "@" : (c.type === "team" ? "🔒" : "#");
    row.innerHTML = `<span class="switcher-hash">${prefix}</span><span></span>`;
    row.querySelector("span:last-child").textContent = isDm
      ? dmLabel(c)
      : c.name;
    row.addEventListener("click", () => {
      selectChannel(c.id);
      el.dialogSwitcher.close();
    });
    el.switcherResults.appendChild(row);
  }
  for (const u of us) {
    const row = document.createElement("div");
    row.className = "switcher-result" + (first ? " active" : "");
    first = false;
    row.appendChild(renderAvatar(u, "xs"));
    const label = document.createElement("span");
    label.textContent = u.displayName + " — start DM";
    row.appendChild(label);
    row.addEventListener("click", async () => {
      el.dialogSwitcher.close();
      await openOrCreateDm(u.uid);
    });
    el.switcherResults.appendChild(row);
  }
  if (chs.length + us.length === 0) {
    const p = document.createElement("p");
    p.className = "hint";
    p.style.padding = "12px";
    p.textContent = "No matches.";
    el.switcherResults.appendChild(p);
  }
}

// ===========================================================================
// #12 Export channel as Markdown
// ===========================================================================

el.btnExportChannel.addEventListener("click", async () => {
  el.userMenuDropdown.hidden = true;
  el.btnUserMenu.setAttribute("aria-expanded", "false");
  await exportCurrentChannel();
});

async function exportCurrentChannel() {
  const ch = state.channels.find((c) => c.id === state.currentChannelId);
  if (!ch) return;
  const lines = [];
  lines.push(`# ${ch.type === "dm" ? "DM with " + dmLabel(ch) : "#" + ch.name}`);
  lines.push("");
  lines.push(`_Exported ${new Date().toLocaleString()}_`);
  lines.push("");
  // Fetch all (up to 1000) messages
  const q = query(
    collection(db, "channels", ch.id, "messages"),
    orderBy("createdAt", "asc"),
    limit(1000),
  );
  const snap = await getDocs(q);
  for (const d of snap.docs) {
    const m = d.data();
    if (m.deleted) continue;
    const when = m.createdAt?.toDate ? m.createdAt.toDate().toLocaleString() : "";
    const author = m.authorName || m.authorEmail || "unknown";
    if (m.type === "poll" && m.poll) {
      lines.push(`**${author}** · _${when}_  📊 **${m.poll.question}**`);
      for (const o of m.poll.options || []) {
        lines.push(`- ${o.label} (${(o.votes || []).length} votes)`);
      }
    } else {
      lines.push(`**${author}** · _${when}_${m.pinned ? " 📌" : ""}`);
      if (m.text) lines.push(m.text);
      if (m.image?.url) lines.push(`[image](${m.image.url})`);
      // Reactions summary
      if (m.reactions) {
        const rx = Object.entries(m.reactions)
          .filter(([, uids]) => (uids || []).length > 0)
          .map(([k, uids]) => `${k} ${uids.length}`).join("  ");
        if (rx) lines.push(`> ${rx}`);
      }
    }
    lines.push("");
  }
  const md = lines.join("\n");
  const blob = new Blob([md], { type: "text/markdown;charset=utf-8" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = `brs-chat-${ch.name || ch.id}-${new Date().toISOString().slice(0, 10)}.md`;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}

// ===========================================================================
// Notification preferences (per-user)
// ===========================================================================

const DEFAULT_NOTIF_PREFS = {
  dm: true,
  mention: true,
  channel: true,
  reactions: false,
  all: false,
  // email flags are saved but not yet delivered — Phase A via Functions/GAS
  emailMention: false,
  emailDm: false,
  emailChannel: false,
};

function getNotifPrefs() {
  // Public prefs (DM/mention/channel/reactions/all) live on users/{uid}.
  // Private bits (webhook URLs) live on userSecrets/{uid} so other users
  // can't read them.
  const pub = state.userDoc?.notificationPrefs || {};
  const secrets = state.userSecrets || {};
  return {
    ...DEFAULT_NOTIF_PREFS,
    ...pub,
    webhooks: secrets.webhooks || pub.webhooks || {},
  };
}

// Start a subscription to the signed-in user's secrets doc.
function subscribeUserSecrets() {
  if (state.unsubSecrets) { state.unsubSecrets(); state.unsubSecrets = null; }
  state.unsubSecrets = onSnapshot(doc(db, "userSecrets", state.user.uid), async (snap) => {
    state.userSecrets = snap.exists() ? snap.data() : {};
    // One-time migration: if old webhooks live on users/{uid}.notificationPrefs.webhooks
    // and userSecrets is empty, copy them over and clear the old location.
    const legacy = state.userDoc?.notificationPrefs?.webhooks;
    if (legacy && !snap.exists()) {
      try {
        await setDoc(doc(db, "userSecrets", state.user.uid), { webhooks: legacy }, { merge: true });
        // Scrub the old field (set empty) so other users can no longer read it.
        await updateDoc(doc(db, "users", state.user.uid), {
          [`notificationPrefs.webhooks`]: {},
        });
        console.log("[migration] moved webhooks to userSecrets");
      } catch (err) {
        console.warn("webhook migration failed", err);
      }
    }
  }, (err) => {
    console.warn("userSecrets subscription failed", err);
  });
}

el.btnNotifPrefs?.addEventListener("click", () => {
  el.userMenuDropdown.hidden = true;
  el.btnUserMenu.setAttribute("aria-expanded", "false");
  const p = getNotifPrefs();
  el.notifDm.checked = p.dm;
  el.notifMention.checked = p.mention;
  el.notifChannel.checked = p.channel;
  el.notifReactions.checked = p.reactions;
  el.notifAll.checked = p.all;
  el.notifEmailMention.checked = p.emailMention;
  el.notifEmailDm.checked = p.emailDm;
  el.notifEmailChannel.checked = p.emailChannel;
  const w = p.webhooks || {};
  el.webhookSlack.value = w.slack || "";
  el.webhookTeams.value = w.teams || "";
  el.webhookDiscord.value = w.discord || "";
  el.webhookDm.checked = w.dm !== false;
  el.webhookMention.checked = w.mention !== false;
  el.webhookChannel.checked = w.channel !== false;
  el.webhookAllReplies.checked = !!w.allReplies;
  el.webhookAll.checked = !!w.all;
  el.webhookTestStatus.textContent = "";
  el.dialogNotifPrefs.showModal();
});
el.btnNotifPrefsCancel?.addEventListener("click", () => el.dialogNotifPrefs.close());
el.formNotifPrefs?.addEventListener("submit", async (e) => {
  e.preventDefault();
  const existing = state.userDoc?.notificationPrefs || {};
  // Public prefs — visible to anyone (no URLs here).
  const prefs = {
    dm: el.notifDm.checked,
    mention: el.notifMention.checked,
    channel: el.notifChannel.checked,
    reactions: el.notifReactions.checked,
    all: el.notifAll.checked,
    emailMention: existing.emailMention ?? DEFAULT_NOTIF_PREFS.emailMention,
    emailDm: existing.emailDm ?? DEFAULT_NOTIF_PREFS.emailDm,
    emailChannel: existing.emailChannel ?? DEFAULT_NOTIF_PREFS.emailChannel,
    // webhooks intentionally NOT stored here anymore.
    webhooks: {},
  };
  // Private — only self can read. Webhook URLs live here.
  const secrets = {
    webhooks: {
      slack: el.webhookSlack.value.trim() || "",
      teams: el.webhookTeams.value.trim() || "",
      discord: el.webhookDiscord.value.trim() || "",
      dm: el.webhookDm.checked,
      mention: el.webhookMention.checked,
      channel: el.webhookChannel.checked,
      allReplies: el.webhookAllReplies.checked,
      all: el.webhookAll.checked,
    },
  };
  try {
    await Promise.all([
      updateDoc(doc(db, "users", state.user.uid), { notificationPrefs: prefs }),
      setDoc(doc(db, "userSecrets", state.user.uid), secrets, { merge: true }),
    ]);
    state.userDoc = { ...state.userDoc, notificationPrefs: prefs };
    state.userSecrets = { ...state.userSecrets, ...secrets };
    el.dialogNotifPrefs.close();
  } catch (err) {
    alert("Save failed: " + err.message);
  }
});

// ===========================================================================
// Webhook forwarding (Slack / Teams / Discord)
// While any BRS Community tab is open, matching events are POSTed directly
// from the browser to the user's personal Slack/Teams/Discord webhook URLs.
// In Phase A this moves server-side (Cloud Function) so it works when the
// user has no tabs open.
// ===========================================================================

async function postWebhook(url, text) {
  if (!url) return;
  try {
    if (url.includes("hooks.slack.com")) {
      // Slack accepts CORS form-encoded POSTs with a `payload` param.
      const body = new URLSearchParams({ payload: JSON.stringify({ text }) });
      await fetch(url, { method: "POST", body, mode: "no-cors" });
    } else if (url.includes("discord.com/api/webhooks") || url.includes("discordapp.com/api/webhooks")) {
      // Discord accepts form-urlencoded content= for simple messages.
      const body = new URLSearchParams({ content: text });
      await fetch(url, { method: "POST", body, mode: "no-cors" });
    } else {
      // Teams (and generic) — expect JSON body.
      await fetch(url, {
        method: "POST",
        mode: "no-cors",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ text }),
      });
    }
  } catch (err) {
    console.warn("webhook post failed", err);
  }
}

// Multi-tab dedup: when a user has the chat open in multiple tabs/devices, all
// tabs see the same Firestore onSnapshot event and would otherwise each fire
// the webhook. We use a Firestore transaction on userSecrets/{uid}/webhookSent/{mid}
// so only the first tab to claim the message sends. Phase A Cloud Function
// replaces this with a single server-side dispatch.
async function tryClaimWebhook(uid, messageId) {
  if (!uid || !messageId) return false;
  const ref = doc(db, "userSecrets", uid, "webhookSent", messageId);
  try {
    return await runTransaction(db, async (tx) => {
      const snap = await tx.get(ref);
      if (snap.exists()) return false;
      tx.set(ref, { ts: serverTimestamp() });
      return true;
    });
  } catch (err) {
    console.warn("[webhook dedup] transaction failed", err);
    return false;
  }
}

async function forwardToWebhooks({ kind, ch, m, permalink }) {
  const w = getNotifPrefs().webhooks || {};
  const hasAny = w.slack || w.teams || w.discord;
  const isReply = !!m.isReply;
  let pass = false;
  if (w.all) pass = true;
  else if (isReply && w.allReplies) pass = true;
  else if (kind === "dm" && w.dm) pass = true;
  else if (kind === "mention" && w.mention) pass = true;
  else if (kind === "channel" && w.channel) pass = true;
  console.log("[webhook]", {
    kind: kind || "(plain)", isReply, hasAny, pass,
    prefs: {
      dm: w.dm, mention: w.mention, channel: w.channel,
      all: w.all, allReplies: w.allReplies,
      slack: !!w.slack, teams: !!w.teams, discord: !!w.discord,
    },
    channel: ch.name || ch.id,
  });
  if (!hasAny || !pass) return;
  if (!(await tryClaimWebhook(state.user?.uid, m.id))) {
    console.log("[webhook dedup] skipped — another tab already claimed", m.id);
    return;
  }
  const chLabel = ch.type === "dm"
    ? `DM from ${m.authorName}`
    : (ch.type === "team" ? `🔒${ch.name}` : `#${ch.name}`);
  const prefix = kind === "mention" ? "@you mentioned"
    : kind === "channel" ? "@channel"
    : kind === "dm" ? "DM"
    : (isReply ? "thread reply" : "message");
  const threadTag = isReply && kind !== null ? " (thread reply)" : "";
  const snippet = (m.text || (m.image ? "[image]" : "[message]")).slice(0, 300);
  const text = `*BRS Community — ${prefix}${threadTag}* in ${chLabel}\n${m.authorName}: ${snippet}\n${permalink}`;
  if (w.slack)   postWebhook(w.slack, text);
  if (w.teams)   postWebhook(w.teams, text);
  if (w.discord) postWebhook(w.discord, text);
}

el.btnWebhookTest?.addEventListener("click", async () => {
  const url = el.webhookSlack.value.trim()
    || el.webhookTeams.value.trim()
    || el.webhookDiscord.value.trim();
  if (!url) {
    el.webhookTestStatus.textContent = "Paste at least one webhook URL first.";
    return;
  }
  el.webhookTestStatus.textContent = "Sending…";
  const text = `✅ Hello from BRS Community! This is a test message for ${state.userDoc?.displayName || "you"}.`;
  const urls = [el.webhookSlack.value, el.webhookTeams.value, el.webhookDiscord.value]
    .map((s) => s.trim()).filter(Boolean);
  for (const u of urls) await postWebhook(u, text);
  el.webhookTestStatus.textContent =
    "Sent. Check your Slack/Teams/Discord. (no-cors mode hides errors; if nothing arrives, double-check the URL.)";
});

// ===========================================================================
// Mentions view
// ===========================================================================

el.btnMentionsView?.addEventListener("click", () => {
  renderMentionsView();
  el.dialogMentionsView.showModal();
});
el.btnMentionsViewClose?.addEventListener("click", () => el.dialogMentionsView.close());

function renderMentionsView() {
  el.mentionsViewList.innerHTML = "";
  const myUid = state.user?.uid;
  if (!myUid) return;
  const items = [];
  for (const [cid, docs] of state.recentMessagesByChannel) {
    const ch = state.channels.find((c) => c.id === cid);
    if (!ch || ch.archived) continue;
    for (const m of docs) {
      if (m.authorUid === myUid || m.deleted) continue;
      const isDm = ch.type === "dm";
      const isMention = (m.mentions || []).includes(myUid);
      const isBroadcast = m.mentionsEveryone === true;
      if (!(isDm || isMention || isBroadcast)) continue;
      items.push({ ch, m, isDm, isMention, isBroadcast });
    }
  }
  items.sort((a, b) => {
    const am = a.m.createdAt?.toMillis?.() ?? 0;
    const bm = b.m.createdAt?.toMillis?.() ?? 0;
    return bm - am;
  });
  const top = items.slice(0, 50);
  if (top.length === 0) {
    el.mentionsViewList.innerHTML = `<p class="hint">No mentions yet. You'll see @you, @channel, and DMs here.</p>`;
    return;
  }
  for (const it of top) {
    const { ch, m, isDm, isMention } = it;
    const item = document.createElement("div");
    item.className = "bookmark-item";
    item.appendChild(renderAvatar(getUserByUid(m.authorUid) || { authorName: m.authorName }, "sm"));
    const bodyEl = document.createElement("div");
    bodyEl.className = "bookmark-item-body";
    const meta = document.createElement("div");
    meta.className = "bookmark-item-meta";
    const kindLabel = isMention ? "@you" : isDm ? "DM" : "@channel";
    const chTitle = ch.type === "dm"
      ? "@" + dmLabel(ch)
      : "#" + ch.name;
    const liveName = getUserByUid(m.authorUid)?.displayName || m.authorName || "?";
    meta.textContent = `${kindLabel} · ${chTitle} · ${liveName} · ${formatTime(m.createdAt)}`;
    const text = document.createElement("div");
    text.innerHTML = renderMessageBody(m.text || (m.image ? "[image]" : "[message]")).html;
    bodyEl.appendChild(meta);
    bodyEl.appendChild(text);
    item.appendChild(bodyEl);
    item.addEventListener("click", () => {
      el.dialogMentionsView.close();
      selectChannel(ch.id);
      state.pendingScrollToMsg = m.id;
    });
    el.mentionsViewList.appendChild(item);
  }
}

// ===========================================================================
// Notifications (Level 1: Browser Notifications API while tab is open)
//
// PHASE A UPGRADE: Replace with Level 2 push notifications once Blaze is active:
//   1. Add manifest.json + service-worker.js for PWA (iOS push requires PWA)
//   2. Enable FCM (Firebase Cloud Messaging)
//   3. Register user FCM tokens into users/{uid}.fcmTokens[] on sign-in
//   4. Cloud Function onMessageCreated() that fans out FCM sends to tagged UIDs
//   5. Service worker handles 'push' event → Notification + click → open channel
//   6. Delete the polling-in-foreground logic below once FCM covers it
// Grep for "PHASE A UPGRADE" to find this block.
// ===========================================================================

const DISMISS_KEY = "brsChat.notifyBannerDismissed";
const channelNotifyListeners = new Map();  // channelId -> unsub
const seenLastMsgId = new Map();            // channelId -> last message id we've "seen"
const seenLastReplyAt = new Map();          // "channelId/parentId" -> lastReplyAt ms we've "seen"

// Attach banner button listeners once at module load.
el.btnEnableNotify?.addEventListener("click", async () => {
  if (!("Notification" in window)) {
    alert("This browser doesn't support notifications.");
    return;
  }
  const perm = await Notification.requestPermission();
  updateNotifyBanner();
  if (perm === "granted") {
    new Notification("BRS Community notifications on", {
      body: "You'll be notified for @mentions, @channel and DMs.",
      icon: "./logo.png",
    });
    // Subscribe this browser to FCM push so it keeps working when the tab is
    // closed. Falls through silently if VAPID key isn't configured yet.
    initFCM().then(registerFCMToken).catch((e) => console.warn("[fcm] init failed", e));
  }
});
el.btnDismissNotify?.addEventListener("click", () => {
  localStorage.setItem(DISMISS_KEY, "1");
  el.notificationBanner.hidden = true;
});

function initNotifications() {
  updateNotifyBanner();
}

function updateNotifyBanner() {
  const supported = "Notification" in window;
  const dismissed = localStorage.getItem(DISMISS_KEY) === "1";
  const permDefault = supported && Notification.permission === "default";
  el.notificationBanner.hidden = !supported || dismissed || !permDefault;
}

// For each channel the user has access to, keep an onSnapshot on recent
// messages (limit 20). Used for:
// - Level 1 browser notifications on new mentions/DMs
// - Tracking unread mention counts per channel
// - Populating the Mentions view
function refreshChannelNotifyListeners() {
  const currentIds = new Set(state.channels.map((c) => c.id));
  for (const [cid, unsub] of channelNotifyListeners) {
    if (!currentIds.has(cid)) {
      unsub();
      channelNotifyListeners.delete(cid);
      seenLastMsgId.delete(cid);
      state.recentMessagesByChannel.delete(cid);
      state.unreadMentionsByChannel.delete(cid);
    }
  }
  for (const ch of state.channels) {
    if (channelNotifyListeners.has(ch.id)) continue;
    const q = query(
      collection(db, "channels", ch.id, "messages"),
      orderBy("createdAt", "desc"),
      limit(20),
    );
    let initialized = false;
    const unsub = onSnapshot(q, (snap) => {
      const docs = snap.docs.map((d) => ({ id: d.id, ...d.data() }));
      state.recentMessagesByChannel.set(ch.id, docs);
      recomputeUnreadMentions(ch.id);
      updateTabTitleAndMentionBadge();
      renderChannelLists();
      if (el.dialogMentionsView?.open) renderMentionsView();
      const latest = docs[0];
      if (!latest) { initialized = true; return; }
      if (!initialized) {
        seenLastMsgId.set(ch.id, latest.id);
        initialized = true;
        return;
      }
      // Also detect thread replies on any message in this channel.
      // Only notify AFTER the initial snapshot completes (so we don't spam
      // on first load for existing replies).
      for (const m of docs) {
        const replyTs = m.lastReplyAt?.toMillis?.() ?? 0;
        if (!replyTs || !m.lastReply) continue;
        const key = ch.id + "/" + m.id;
        const seenTs = seenLastReplyAt.get(key) || 0;
        if (replyTs > seenTs) {
          const wasFirstTime = !seenLastReplyAt.has(key);
          seenLastReplyAt.set(key, replyTs);
          console.log("[reply-detect]", {
            channel: ch.name, parentId: m.id,
            replyTs, seenTs, initialized, wasFirstTime,
            willNotify: initialized,
          });
          if (initialized) {
            // Synthesize a message-shaped object so maybeNotify / webhook
            // forwarding can use the normal code path.
            const synth = {
              id: m.id,
              text: m.lastReply.text,
              authorUid: m.lastReply.authorUid,
              authorName: m.lastReply.authorName,
              authorEmail: m.lastReply.authorEmail,
              mentions: m.lastReply.mentions || [],
              mentionsEveryone: !!m.lastReply.mentionsEveryone,
              createdAt: m.lastReplyAt,
              isReply: true,
              parentMessageId: m.id,
            };
            maybeNotify(ch, synth);
          }
        }
      }

      if (seenLastMsgId.get(ch.id) === latest.id) return;
      seenLastMsgId.set(ch.id, latest.id);
      maybeNotify(ch, latest);
    }, (err) => {
      console.warn("notify listener failed for", ch.id, err);
    });
    channelNotifyListeners.set(ch.id, unsub);
  }
}

function tearDownChannelNotifyListeners() {
  for (const unsub of channelNotifyListeners.values()) unsub();
  channelNotifyListeners.clear();
  seenLastMsgId.clear();
  seenLastReplyAt.clear();
  state.recentMessagesByChannel.clear();
  state.unreadMentionsByChannel.clear();
  updateTabTitleAndMentionBadge();
}

function recomputeUnreadMentions(channelId) {
  const docs = state.recentMessagesByChannel.get(channelId) || [];
  const lastRead = state.lastReadByChannel[channelId];
  const lastReadMs = lastRead?.toMillis ? lastRead.toMillis()
    : (lastRead?.seconds ? lastRead.seconds * 1000 : 0);
  let count = 0;
  for (const m of docs) {
    if (m.authorUid === state.user?.uid) continue;
    if (m.deleted) continue;
    const ms = m.createdAt?.toMillis ? m.createdAt.toMillis() : 0;
    if (ms <= lastReadMs) continue;
    const ch = state.channels.find((c) => c.id === channelId);
    const isDm = ch?.type === "dm";
    const isMention = (m.mentions || []).includes(state.user.uid);
    const isBroadcast = m.mentionsEveryone === true;
    if (isDm || isMention || isBroadcast) count++;
  }
  state.unreadMentionsByChannel.set(channelId, count);
}

function totalUnreadMentions() {
  let sum = 0;
  for (const n of state.unreadMentionsByChannel.values()) sum += n;
  return sum;
}

function updateTabTitleAndMentionBadge() {
  const total = totalUnreadMentions();
  document.title = total > 0 ? `(${total}) ${BASE_TAB_TITLE}` : BASE_TAB_TITLE;
  if (el.mentionsTotal) {
    el.mentionsTotal.textContent = total;
    el.mentionsTotal.hidden = total === 0;
  }
}

function maybeNotify(ch, m) {
  if (!m || m.deleted) return;

  const isSelf = m.authorUid === state.user.uid;
  const isMention = (m.mentions || []).includes(state.user.uid);
  const isBroadcast = m.mentionsEveryone === true;
  const isDm = ch.type === "dm";
  const kind = isDm ? "dm" : isMention ? "mention" : (isBroadcast ? "channel" : null);
  console.log("[notify]", {
    channel: ch.name || ch.id,
    kind: kind || "(none)",
    self: isSelf,
    author: m.authorName,
    text: (m.text || "").slice(0, 60),
    mentions: m.mentions,
    mentionsEveryone: m.mentionsEveryone,
  });
  if (isSelf) return;         // never notify yourself about your own message

  // (1) Webhook forwarding — always tried; the function applies its own
  //     per-event filter (dm / mention / channel / allReplies / all).
  forwardToWebhooks({ kind, ch, m, permalink: channelPermalink(ch.id, m.id) });

  // (2) Browser desktop notification — only meaningful for DM/mention/channel
  //     kinds; plain messages never pop desktop notifications even if the
  //     webhook is opted in to "everything".
  if (!kind) return;
  if (!("Notification" in window) || Notification.permission !== "granted") return;
  if (document.hasFocus() && state.currentChannelId === ch.id) return;

  const prefs = getNotifPrefs();
  const passes =
    (prefs.all) ||
    (isDm && prefs.dm) ||
    (isMention && prefs.mention) ||
    (isBroadcast && prefs.channel);
  if (!passes) return;

  const author = getUserByUid(m.authorUid);
  const chTitle = ch.type === "dm"
    ? dmLabel(ch)
    : ("#" + ch.name);
  const title = `${author?.displayName || m.authorName || "New message"}  ·  ${chTitle}`;
  const body = m.text || (m.image ? "[image]" : "");
  try {
    const n = new Notification(title, {
      body: body.slice(0, 180),
      icon: author?.photoURL || "./logo.png",
      badge: "./logo.png",
      tag: ch.id,  // collapse multiple into one per channel
      renotify: false,
    });
    n.onclick = () => {
      window.focus();
      selectChannel(ch.id);
      n.close();
    };
  } catch (err) {
    console.warn("notification failed", err);
  }
}

// ===========================================================================
// FCM (Phase A push). Subscribes the browser/SW to Firebase Cloud Messaging
// so that pushes from the sendPushOnNewMessage Cloud Function reach this
// device even when the tab is closed.
//
// In foreground we still rely on the existing Firestore snapshot listener
// (maybeNotify) to surface new messages — FCM's onMessage fires too, but we
// keep that path quiet to avoid double notifications. The SW handles the
// background case alone.
// ===========================================================================

let fcmMessaging = null;
let fcmReady = false;
let fcmSwRegistration = null;
const FCM_TOKEN_LOCAL_KEY = "brsChat.fcmToken";

async function initFCM() {
  if (fcmReady) return;
  if (!FCM_VAPID_KEY) {
    console.info("[fcm] disabled: FCM_VAPID_KEY is empty in firebase-config.js");
    return;
  }
  if (!("serviceWorker" in navigator)) {
    console.info("[fcm] disabled: this browser does not support service workers");
    return;
  }
  let supported = false;
  try { supported = await isMessagingSupported(); } catch (_) { supported = false; }
  if (!supported) {
    console.info("[fcm] disabled: messaging not supported in this browser");
    return;
  }

  try {
    fcmSwRegistration = await navigator.serviceWorker.register(
      "./firebase-messaging-sw.js",
      { scope: "./" },
    );
  } catch (err) {
    console.warn("[fcm] sw registration failed", err);
    return;
  }

  fcmMessaging = getMessaging(app);
  fcmReady = true;
  console.info("[fcm] initialized (sw registered, ready to subscribe)");

  // Foreground messages — log only. The Firestore snapshot listener
  // already updates the UI and the existing maybeNotify shows the in-tab
  // Notification when appropriate.
  onMessage(fcmMessaging, (payload) => {
    console.log("[fcm] foreground", payload);
  });

  // SW → page postMessage when a notification is clicked.
  navigator.serviceWorker.addEventListener("message", (event) => {
    if (event.data?.type === "fcm-notification-click" && event.data.channelId) {
      try { selectChannel(event.data.channelId); } catch (e) { console.warn(e); }
    }
  });
}

// Register the current browser's FCM token under users/{uid}.fcmTokens.
// Called when the user grants Notification permission, and again on every
// sign-in (token may rotate or have been removed by the browser).
async function registerFCMToken() {
  if (!fcmReady) {
    console.info("[fcm] cannot register: not initialized yet");
    return;
  }
  if (!state.user) {
    console.info("[fcm] cannot register: no signed-in user");
    return;
  }
  if (typeof Notification === "undefined" || Notification.permission !== "granted") {
    console.info("[fcm] cannot register: notification permission =",
      typeof Notification !== "undefined" ? Notification.permission : "(unsupported)");
    return;
  }
  try {
    const token = await getToken(fcmMessaging, {
      vapidKey: FCM_VAPID_KEY,
      serviceWorkerRegistration: fcmSwRegistration,
    });
    if (!token) {
      console.warn("[fcm] getToken returned empty");
      return;
    }
    const tokenId = (await sha256Hex(token)).slice(0, 16);
    await updateDoc(doc(db, "users", state.user.uid), {
      [`fcmTokens.${tokenId}`]: {
        token,
        ua: (navigator.userAgent || "").slice(0, 200),
        createdAt: serverTimestamp(),
        lastUsedAt: serverTimestamp(),
      },
    });
    localStorage.setItem(FCM_TOKEN_LOCAL_KEY, tokenId);
    console.log("[fcm] token registered", tokenId);
  } catch (err) {
    console.warn("[fcm] getToken failed", err);
  }
}

// Best-effort cleanup on explicit sign-out. Removes this browser's token
// from the user doc so the Cloud Function stops targeting it.
async function unregisterFCMToken() {
  const tokenId = localStorage.getItem(FCM_TOKEN_LOCAL_KEY);
  if (!tokenId) return;
  localStorage.removeItem(FCM_TOKEN_LOCAL_KEY);
  if (fcmReady) {
    try { await deleteToken(fcmMessaging); } catch (_) {}
  }
  if (state.user) {
    try {
      await updateDoc(doc(db, "users", state.user.uid), {
        [`fcmTokens.${tokenId}`]: deleteField(),
      });
    } catch (_) {}
  }
}

// ===========================================================================
// Invite-password gate
// ---------------------------------------------------------------------------
// First-time visitors enter a shared password before the sign-in screen is
// shown. The hash lives in Firestore at config/invite.hash (public read so
// the gate works pre-auth) and is rotatable by admins. localStorage stores
// the last hash the visitor cleared; mismatch against the current hash forces
// re-entry (which is how rotation broadcasts).
// ===========================================================================

const INVITE_PASSED_KEY = "brsInvitePassedHash";

// Gate state lifecycle:
//   "checking" — initial; auth listener waits on `gateResolved` before any UI
//   "open"     — gate cleared (or no gate configured) — sign-in / chat allowed
//   "closed"   — gate not yet cleared — only the invite screen is shown
let gateState = "checking";
let _gateResolve;
const gateResolved = new Promise((r) => { _gateResolve = r; });

async function sha256Hex(text) {
  const buf = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(text));
  return [...new Uint8Array(buf)].map((b) => b.toString(16).padStart(2, "0")).join("");
}

async function fetchInviteHash() {
  try {
    const snap = await getDoc(doc(db, "config", "invite"));
    return snap.exists() ? (snap.data().hash || "") : "";
  } catch (e) {
    console.warn("Could not read config/invite (gate disabled):", e);
    return "";
  }
}

// Decides gate state and resolves the gateResolved promise so the auth
// observer can proceed.
async function checkInviteGate() {
  const currentHash = await fetchInviteHash();
  if (!currentHash) {
    gateState = "open";  // no gate configured
    _gateResolve();
    return;
  }
  const stored = localStorage.getItem(INVITE_PASSED_KEY);
  if (stored && stored === currentHash) {
    gateState = "open";
    _gateResolve();
    return;
  }
  // Else: gate must be cleared.
  gateState = "closed";
  el.inviteScreen._currentHash = currentHash;
  showScreen("invite");
  setTimeout(() => el.inputInvite?.focus(), 50);
  _gateResolve();
}

el.formInvite?.addEventListener("submit", async (e) => {
  e.preventDefault();
  el.inviteError.hidden = true;
  const entered = el.inputInvite.value;
  if (!entered) return;
  const expected = el.inviteScreen._currentHash || (await fetchInviteHash());
  const actual = await sha256Hex(entered);
  if (actual === expected) {
    localStorage.setItem(INVITE_PASSED_KEY, expected);
    el.inputInvite.value = "";
    gateState = "open";
    // Re-run the auth observer's logic with the current user. If signed in
    // already, this jumps straight into chat; otherwise it shows the sign-in
    // screen.
    await processAuthState(auth.currentUser);
  } else {
    el.inviteError.textContent = "Wrong password.";
    el.inviteError.hidden = false;
  }
});

// ===========================================================================
// Maintenance — admin emergency controls
// ===========================================================================

function setMaintenanceStatus(text, isError) {
  if (!el.maintenanceStatus) return;
  el.maintenanceStatus.textContent = text;
  el.maintenanceStatus.style.color = isError ? "var(--danger)" : "";
}

el.toggleMaintenance?.addEventListener("change", async () => {
  if (!isAdmin()) { el.toggleMaintenance.checked = !el.toggleMaintenance.checked; return; }
  try {
    await writeMaintenance({ active: el.toggleMaintenance.checked });
    setMaintenanceStatus(el.toggleMaintenance.checked ? "Read-only mode ON." : "Read-only mode OFF.");
  } catch (err) {
    el.toggleMaintenance.checked = !el.toggleMaintenance.checked;
    setMaintenanceStatus("Failed: " + err.message, true);
  }
});

el.toggleSigninLockdown?.addEventListener("change", async () => {
  if (!isAdmin()) { el.toggleSigninLockdown.checked = !el.toggleSigninLockdown.checked; return; }
  try {
    await writeMaintenance({ signInDisabled: el.toggleSigninLockdown.checked });
    setMaintenanceStatus(el.toggleSigninLockdown.checked ? "Sign-in lockdown ON." : "Sign-in lockdown OFF.");
  } catch (err) {
    el.toggleSigninLockdown.checked = !el.toggleSigninLockdown.checked;
    setMaintenanceStatus("Failed: " + err.message, true);
  }
});

el.inputMaintenanceMsg?.addEventListener("change", async () => {
  if (!isAdmin()) return;
  try {
    await writeMaintenance({ message: el.inputMaintenanceMsg.value.trim() });
    setMaintenanceStatus("Banner message updated.");
  } catch (err) {
    setMaintenanceStatus("Failed: " + err.message, true);
  }
});

el.btnPauseNow?.addEventListener("click", async () => {
  if (!isAdmin()) return;
  if (!confirm("PAUSE the chat now?\n\nAll non-admin writes will be blocked and new sign-ins rejected. Existing sessions stay active until they sign out.")) return;
  try {
    await writeMaintenance({
      active: true,
      signInDisabled: true,
      message: el.inputMaintenanceMsg?.value.trim() || "Chat paused — investigating.",
    });
    setMaintenanceStatus("PAUSED. Read-only + sign-in lockdown active.");
  } catch (err) {
    setMaintenanceStatus("Failed: " + err.message, true);
  }
});

el.btnResumeNow?.addEventListener("click", async () => {
  if (!isAdmin()) return;
  if (!confirm("Resume normal operation?")) return;
  try {
    await writeMaintenance({ active: false, signInDisabled: false, message: "" });
    setMaintenanceStatus("Resumed. Chat is fully operational.");
  } catch (err) {
    setMaintenanceStatus("Failed: " + err.message, true);
  }
});

// Admin panel: rotate invite password.
el.btnRotateInvite?.addEventListener("click", async () => {
  if (!isAdmin()) return;
  const newPw = (el.inputNewInvite?.value || "").trim();
  el.inviteRotateStatus.textContent = "";
  el.inviteRotateStatus.style.color = "";
  try {
    if (!newPw) {
      // Empty = disable gate.
      if (!confirm("Disable the invite gate? Anyone with the URL will reach the sign-in screen.")) return;
      await setDoc(doc(db, "config", "invite"), { hash: "", updatedAt: serverTimestamp() }, { merge: true });
      el.inviteRotateStatus.textContent = "Gate disabled.";
    } else {
      if (newPw.length < 6) {
        el.inviteRotateStatus.textContent = "Password too short (min 6 chars).";
        el.inviteRotateStatus.style.color = "var(--danger)";
        return;
      }
      const hash = await sha256Hex(newPw);
      await setDoc(doc(db, "config", "invite"), { hash, updatedAt: serverTimestamp() }, { merge: true });
      // Keep the admin themself signed in by storing the new hash.
      localStorage.setItem(INVITE_PASSED_KEY, hash);
      el.inviteRotateStatus.textContent = "Updated. All other devices will be asked for the new password on next visit.";
    }
    if (el.inputNewInvite) el.inputNewInvite.value = "";
  } catch (err) {
    el.inviteRotateStatus.textContent = "Failed: " + err.message;
    el.inviteRotateStatus.style.color = "var(--danger)";
  }
});

// Admin panel: export image list for backup. Returns a JSON dump so the
// admin can hand it off to a backup script that downloads the originals
// from Cloudinary and uploads them elsewhere (e.g. Google Drive).
el.btnExportImageList?.addEventListener("click", async () => {
  if (!isAdmin()) return;
  el.imageListStatus.textContent = "Loading…";
  el.imageListStatus.style.color = "";
  el.imageListOutput.hidden = true;
  el.imageListOutput.value = "";
  try {
    const fn = httpsCallable(functions, "listImagesForBackup");
    const { data } = await fn({});
    const json = JSON.stringify(data, null, 2);
    el.imageListOutput.value = json;
    el.imageListOutput.hidden = false;
    el.imageListOutput.select();
    el.imageListStatus.textContent =
      `OK: ${data.items?.length ?? 0} image(s) across ${Object.keys(data.channels || {}).length} channel(s). Copy the textbox below.`;
  } catch (err) {
    el.imageListStatus.textContent = "Failed: " + err.message;
    el.imageListStatus.style.color = "var(--danger)";
  }
});

// ===========================================================================
// Bootstrap
// ===========================================================================

(async function bootstrap() {
  // 1. Email-link callback first (must run regardless of gate so the URL is
  //    not stranded if a user clicks the link from a fresh device).
  await handleEmailLinkReturn();
  // 2. Gate. If passed, onAuthStateChanged will route to signin/profile/chat.
  //    If not passed, invite screen is shown until they clear it.
  await checkInviteGate();
})();
