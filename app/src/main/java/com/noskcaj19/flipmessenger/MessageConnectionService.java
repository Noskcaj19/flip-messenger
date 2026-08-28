package com.noskcaj19.flipmessenger;

import android.app.Notification;
import android.app.NotificationChannel;
import android.app.NotificationManager;
import android.app.PendingIntent;
import android.app.Service;
import android.content.Intent;
import android.content.SharedPreferences;
import android.os.IBinder;
import android.util.Log;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public final class MessageConnectionService extends Service {
    private static final String TAG = "FlipMessenger";
    private static final String SERVICE_CHANNEL = "message_connection";
    private static final String MESSAGE_CHANNEL = "new_messages";
    private static final int SERVICE_NOTIFICATION_ID = 1;
    private static final int LONG_POLL_SECONDS = 25;

    private final ExecutorService network = Executors.newSingleThreadExecutor();
    private final Map<String, String> channelNames = new HashMap<>();
    private volatile boolean stopped;

    @Override
    public void onCreate() {
        super.onCreate();
        createNotificationChannels();
        startForeground(SERVICE_NOTIFICATION_ID, serviceNotification("Listening for messages"));
        network.execute(this::listen);
    }

    @Override
    public int onStartCommand(Intent intent, int flags, int startId) {
        return START_STICKY;
    }

    @Override
    public void onDestroy() {
        stopped = true;
        network.shutdownNow();
        super.onDestroy();
    }

    @Override
    public IBinder onBind(Intent intent) {
        return null;
    }

    private void listen() {
        ApiClient api;
        try {
            api = new ApiClient(BuildConfig.SERVER_URL, BuildConfig.API_TOKEN);
        } catch (IllegalArgumentException error) {
            Log.e(TAG, "background connection configuration failed", error);
            stopSelf();
            return;
        }

        SharedPreferences preferences = getSharedPreferences("message_connection", MODE_PRIVATE);
        String cursor = preferences.getString("cursor", null);
        int failures = 0;
        while (!stopped) {
            try {
                if (cursor == null) {
                    Snapshot snapshot = api.bootstrap();
                    rememberChannels(snapshot.channels);
                    cursor = snapshot.cursor;
                    preferences.edit().putString("cursor", cursor).apply();
                } else {
                    SyncResult result = api.sync(cursor, LONG_POLL_SECONDS);
                    rememberChannels(result.channels);
                    for (Message message : result.messages) {
                        if (!"self".equals(message.author) && !MainActivity.isVisible()) {
                            showMessageNotification(message);
                        }
                    }
                    cursor = result.cursor;
                    preferences.edit().putString("cursor", cursor).apply();
                }
                failures = 0;
            } catch (Exception error) {
                failures++;
                long delaySeconds = Math.min(30, 1L << Math.min(failures, 5));
                Log.w(TAG, "background sync failed; retrying in " + delaySeconds + "s", error);
                updateServiceNotification("Reconnecting…");
                try {
                    TimeUnit.SECONDS.sleep(delaySeconds);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    return;
                }
                updateServiceNotification("Listening for messages");
            }
        }
    }

    private void rememberChannels(Iterable<Channel> channels) {
        if (channels == null) {
            return;
        }
        for (Channel channel : channels) {
            channelNames.put(channel.id, channel.displayName);
        }
    }

    private void createNotificationChannels() {
        NotificationManager notifications = getSystemService(NotificationManager.class);
        NotificationChannel connection = new NotificationChannel(
                SERVICE_CHANNEL, "Message connection", NotificationManager.IMPORTANCE_LOW);
        connection.setDescription("Keeps Flip Messenger connected to its server");
        connection.setShowBadge(false);
        notifications.createNotificationChannel(connection);

        NotificationChannel messages = new NotificationChannel(
                MESSAGE_CHANNEL, "New messages", NotificationManager.IMPORTANCE_HIGH);
        messages.setDescription("Incoming Flip Messenger messages");
        notifications.createNotificationChannel(messages);
    }

    private Notification serviceNotification(String text) {
        return new Notification.Builder(this, SERVICE_CHANNEL)
                .setSmallIcon(android.R.drawable.stat_notify_sync_noanim)
                .setContentTitle("Flip Messenger")
                .setContentText(text)
                .setContentIntent(openAppIntent())
                .setOngoing(true)
                .setOnlyAlertOnce(true)
                .setCategory(Notification.CATEGORY_SERVICE)
                .build();
    }

    private void updateServiceNotification(String text) {
        getSystemService(NotificationManager.class)
                .notify(SERVICE_NOTIFICATION_ID, serviceNotification(text));
    }

    private void showMessageNotification(Message message) {
        String title = channelNames.get(message.channelId);
        if (title == null || title.isEmpty()) {
            title = "New message";
        }
        Notification notification = new Notification.Builder(this, MESSAGE_CHANNEL)
                .setSmallIcon(android.R.drawable.stat_notify_chat)
                .setContentTitle(title)
                .setContentText(message.text)
                .setStyle(new Notification.BigTextStyle().bigText(message.text))
                .setContentIntent(openAppIntent())
                .setAutoCancel(true)
                .setCategory(Notification.CATEGORY_MESSAGE)
                .build();
        int notificationId = 1_000 + (message.id.hashCode() & 0x3fffffff);
        getSystemService(NotificationManager.class).notify(notificationId, notification);
    }

    private PendingIntent openAppIntent() {
        Intent intent = new Intent(this, MainActivity.class)
                .setFlags(Intent.FLAG_ACTIVITY_CLEAR_TOP | Intent.FLAG_ACTIVITY_SINGLE_TOP);
        return PendingIntent.getActivity(
                this, 0, intent, PendingIntent.FLAG_UPDATE_CURRENT | PendingIntent.FLAG_IMMUTABLE);
    }
}
