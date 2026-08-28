package com.noskcaj19.flipmessenger;

import android.app.Activity;
import android.graphics.Color;
import android.graphics.Typeface;
import android.os.Bundle;
import android.text.InputType;
import android.util.Log;
import android.view.Gravity;
import android.view.InputDevice;
import android.view.KeyEvent;
import android.view.View;
import android.view.ViewGroup;
import android.widget.EditText;
import android.widget.LinearLayout;
import android.widget.ScrollView;
import android.widget.TableLayout;
import android.widget.TableRow;
import android.widget.TextView;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public final class MainActivity extends Activity {
    private static final String TAG = "FlipMessenger";

    private final List<Channel> channels = new ArrayList<>();
    private final List<Message> messages = new ArrayList<>();
    private final Set<String> messageIds = new HashSet<>();
    private final ScheduledExecutorService network = Executors.newSingleThreadScheduledExecutor();

    private ApiClient api;
    private ScheduledFuture<?> polling;
    private TextView heading;
    private TextView content;
    private ScrollView channelListScroll;
    private TableLayout channelTable;
    private View selectedChannelRow;
    private TextView status;
    private EditText composer;
    private int selectedChannel;
    private Channel openChannel;
    private volatile String cursor;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(createContentView());
        try {
            api = new ApiClient(BuildConfig.SERVER_URL, BuildConfig.API_TOKEN);
        } catch (IllegalArgumentException error) {
            showStatus(error.getMessage());
        }
    }

    @Override
    protected void onResume() {
        super.onResume();
        if (api != null && (polling == null || polling.isDone())) {
            polling = network.scheduleWithFixedDelay(this::poll, 0, 2, TimeUnit.SECONDS);
        }
    }

    @Override
    protected void onPause() {
        if (polling != null) {
            polling.cancel(false);
            polling = null;
        }
        super.onPause();
    }

    @Override
    protected void onDestroy() {
        network.shutdownNow();
        super.onDestroy();
    }

    @Override
    public boolean dispatchKeyEvent(KeyEvent event) {
        Log.i(TAG, describeForLog(event));
        int keyCode = event.getKeyCode();
        if (event.getAction() != KeyEvent.ACTION_DOWN || event.getRepeatCount() != 0) {
            return isAppControlKey(keyCode) || super.dispatchKeyEvent(event);
        }

        if (keyCode == KeyEvent.KEYCODE_CALL) {
            showStatus("Voice calls are not implemented yet");
            return true;
        }
        if (openChannel == null) {
            if (keyCode == KeyEvent.KEYCODE_DPAD_UP) {
                moveSelection(-1);
                return true;
            }
            if (keyCode == KeyEvent.KEYCODE_DPAD_DOWN) {
                moveSelection(1);
                return true;
            }
            if (keyCode == KeyEvent.KEYCODE_DPAD_CENTER
                    || keyCode == KeyEvent.KEYCODE_ENTER
                    || keyCode == KeyEvent.KEYCODE_SOFT_RIGHT) {
                openSelectedChannel();
                return true;
            }
        } else {
            if (keyCode == KeyEvent.KEYCODE_BACK || keyCode == KeyEvent.KEYCODE_SOFT_LEFT) {
                showChannels();
                return true;
            }
            if (keyCode == KeyEvent.KEYCODE_DPAD_CENTER) {
                sendComposedMessage();
                return true;
            }
        }
        return super.dispatchKeyEvent(event);
    }

    private static boolean isAppControlKey(int keyCode) {
        return keyCode == KeyEvent.KEYCODE_DPAD_UP
                || keyCode == KeyEvent.KEYCODE_DPAD_DOWN
                || keyCode == KeyEvent.KEYCODE_DPAD_CENTER
                || keyCode == KeyEvent.KEYCODE_ENTER
                || keyCode == KeyEvent.KEYCODE_BACK
                || keyCode == KeyEvent.KEYCODE_SOFT_LEFT
                || keyCode == KeyEvent.KEYCODE_SOFT_RIGHT
                || keyCode == KeyEvent.KEYCODE_CALL;
    }

    private LinearLayout createContentView() {
        LinearLayout root = new LinearLayout(this);
        root.setLayoutParams(new ViewGroup.LayoutParams(
                ViewGroup.LayoutParams.MATCH_PARENT,
                ViewGroup.LayoutParams.MATCH_PARENT));
        root.setOrientation(LinearLayout.VERTICAL);
        root.setPadding(dp(10), dp(8), dp(10), dp(8));
        root.setBackgroundColor(Color.rgb(17, 24, 39));

        heading = text("CHANNELS", 16, Color.rgb(34, 211, 238));
        heading.setTypeface(Typeface.DEFAULT_BOLD);
        root.addView(heading);

        channelTable = new TableLayout(this);
        channelTable.setStretchAllColumns(true);

        channelListScroll = new ScrollView(this);
        channelListScroll.setFillViewport(true);
        channelListScroll.setVisibility(View.GONE);
        channelListScroll.addView(channelTable, new ScrollView.LayoutParams(
                ViewGroup.LayoutParams.MATCH_PARENT,
                ViewGroup.LayoutParams.WRAP_CONTENT));
        root.addView(channelListScroll, new LinearLayout.LayoutParams(
                ViewGroup.LayoutParams.MATCH_PARENT, 0, 1f));

        content = text("Connecting to server…", 15, Color.WHITE);
        content.setGravity(Gravity.TOP | Gravity.START);
        content.setTypeface(Typeface.MONOSPACE);
        content.setLayoutParams(new LinearLayout.LayoutParams(
                ViewGroup.LayoutParams.MATCH_PARENT, 0, 1f));
        root.addView(content);

        composer = new EditText(this);
        composer.setSingleLine(true);
        composer.setTextSize(14);
        composer.setTextColor(Color.WHITE);
        composer.setHintTextColor(Color.rgb(156, 163, 175));
        composer.setHint("Type, then press center");
        composer.setInputType(InputType.TYPE_CLASS_TEXT | InputType.TYPE_TEXT_FLAG_CAP_SENTENCES);
        composer.setBackgroundColor(Color.rgb(31, 41, 55));
        composer.setPadding(dp(6), 0, dp(6), 0);
        composer.setVisibility(View.GONE);
        root.addView(composer, new LinearLayout.LayoutParams(
                ViewGroup.LayoutParams.MATCH_PARENT, dp(38)));

        status = text("Starting…", 10, Color.rgb(156, 163, 175));
        status.setGravity(Gravity.CENTER_VERTICAL | Gravity.START);
        root.addView(status, new LinearLayout.LayoutParams(
                ViewGroup.LayoutParams.MATCH_PARENT, dp(24)));

        return root;
    }

    private void poll() {
        try {
            if (cursor == null) {
                Snapshot snapshot = api.bootstrap();
                runOnUiThread(() -> applySnapshot(snapshot));
            } else {
                SyncResult result = api.sync(cursor);
                runOnUiThread(() -> applySync(result));
            }
        } catch (Exception error) {
            Log.w(TAG, "sync failed", error);
            showStatus("Offline: " + shortError(error));
        }
    }

    private void applySnapshot(Snapshot snapshot) {
        channels.clear();
        channels.addAll(snapshot.channels);
        messages.clear();
        messageIds.clear();
        addMessages(snapshot.messages);
        cursor = snapshot.cursor;
        selectedChannel = Math.min(selectedChannel, Math.max(0, channels.size() - 1));
        render();
        showStatus("Online • cursor " + cursor);
    }

    private void applySync(SyncResult result) {
        if (result.channels != null) {
            String openChannelId = openChannel == null ? null : openChannel.id;
            channels.clear();
            channels.addAll(result.channels);
            openChannel = findChannel(openChannelId);
            selectedChannel = Math.min(selectedChannel, Math.max(0, channels.size() - 1));
        }
        addMessages(result.messages);
        cursor = result.cursor;
        render();
        showStatus("Online • cursor " + cursor);
    }

    private Channel findChannel(String id) {
        if (id == null) {
            return null;
        }
        for (Channel channel : channels) {
            if (id.equals(channel.id)) {
                return channel;
            }
        }
        return null;
    }

    private void addMessages(List<Message> additions) {
        for (Message message : additions) {
            if (messageIds.add(message.id)) {
                messages.add(message);
            }
        }
    }

    private void render() {
        if (openChannel == null) {
            renderChannels();
        } else {
            renderConversation();
        }
    }

    private void renderChannels() {
        heading.setText("CHANNELS");
        composer.setVisibility(View.GONE);
        content.setVisibility(View.GONE);
        channelListScroll.setVisibility(View.VISIBLE);
        channelTable.removeAllViews();
        selectedChannelRow = null;

        if (channels.isEmpty()) {
            TextView empty = createChannelCell("No channels configured", false);
            channelTable.addView(empty, new TableLayout.LayoutParams(
                    ViewGroup.LayoutParams.MATCH_PARENT,
                    ViewGroup.LayoutParams.WRAP_CONTENT));
            return;
        }

        for (int i = 0; i < channels.size(); i++) {
            Channel channel = channels.get(i);
            if (i > 0) {
                View separator = new View(this);
                separator.setBackgroundColor(Color.rgb(55, 65, 81));
                channelTable.addView(separator, new TableLayout.LayoutParams(
                        ViewGroup.LayoutParams.MATCH_PARENT, dp(1)));
            }

            boolean selected = i == selectedChannel;
            TableRow row = new TableRow(this);
            row.setBackgroundColor(selected ? Color.rgb(31, 41, 55) : Color.TRANSPARENT);
            row.addView(createChannelCell(
                    (selected ? "▶ " : "  ") + channel.displayName,
                    selected),
                    new TableRow.LayoutParams(0, ViewGroup.LayoutParams.WRAP_CONTENT, 1f));
            channelTable.addView(row, new TableLayout.LayoutParams(
                    ViewGroup.LayoutParams.MATCH_PARENT,
                    ViewGroup.LayoutParams.WRAP_CONTENT));
            if (selected) {
                selectedChannelRow = row;
            }
        }
        channelListScroll.post(() -> {
            if (openChannel == null) {
                scrollSelectedChannelIntoView();
            }
        });
    }

    private TextView createChannelCell(String value, boolean selected) {
        TextView cell = text(value, 15, Color.WHITE);
        cell.setTypeface(Typeface.MONOSPACE, selected ? Typeface.BOLD : Typeface.NORMAL);
        cell.setGravity(Gravity.CENTER_VERTICAL | Gravity.START);
        cell.setSingleLine(false);
        cell.setHorizontallyScrolling(false);
        cell.setPadding(dp(4), dp(7), dp(4), dp(7));
        return cell;
    }

    private void scrollSelectedChannelIntoView() {
        if (selectedChannelRow == null) {
            return;
        }

        int itemTop = selectedChannelRow.getTop();
        int itemBottom = selectedChannelRow.getBottom();
        int viewportHeight = channelListScroll.getHeight()
                - channelListScroll.getPaddingTop()
                - channelListScroll.getPaddingBottom();
        int scrollY = channelListScroll.getScrollY();
        int targetY = scrollY;
        if (itemTop < scrollY) {
            targetY = itemTop;
        } else if (itemBottom > scrollY + viewportHeight) {
            targetY = itemBottom - viewportHeight;
        }

        int maxScrollY = Math.max(0, channelTable.getHeight() - viewportHeight);
        channelListScroll.scrollTo(0, Math.max(0, Math.min(targetY, maxScrollY)));
    }

    private void renderConversation() {
        heading.setText(openChannel.displayName);
        composer.setVisibility(View.VISIBLE);
        channelListScroll.setVisibility(View.GONE);
        content.setVisibility(View.VISIBLE);
        content.scrollTo(0, 0);
        List<Message> visible = new ArrayList<>();
        for (Message message : messages) {
            if (openChannel.id.equals(message.channelId)) {
                visible.add(message);
            }
        }
        int start = Math.max(0, visible.size() - 7);
        StringBuilder transcript = new StringBuilder();
        for (int i = start; i < visible.size(); i++) {
            Message message = visible.get(i);
            transcript.append("self".equals(message.author) ? "Me: " : "Them: ")
                    .append(message.text)
                    .append('\n');
        }
        content.setText(transcript.length() == 0 ? "No messages yet" : transcript.toString());
    }

    private void moveSelection(int amount) {
        if (channels.isEmpty()) {
            return;
        }
        selectedChannel = (selectedChannel + amount + channels.size()) % channels.size();
        renderChannels();
    }

    private void openSelectedChannel() {
        if (channels.isEmpty()) {
            return;
        }
        openChannel = channels.get(selectedChannel);
        renderConversation();
        composer.requestFocus();
    }

    private void showChannels() {
        openChannel = null;
        composer.clearFocus();
        renderChannels();
    }

    private void sendComposedMessage() {
        String text = composer.getText().toString().trim();
        if (text.isEmpty() || openChannel == null) {
            return;
        }
        String channelId = openChannel.id;
        String commandId = "cmd_" + UUID.randomUUID();
        String clientMessageId = "cmsg_" + UUID.randomUUID();
        composer.setText("");
        showStatus("Sending…");
        network.execute(() -> {
            try {
                api.sendMessage(commandId, clientMessageId, channelId, text);
                SyncResult result = api.sync(cursor);
                runOnUiThread(() -> applySync(result));
            } catch (Exception error) {
                Log.w(TAG, "send failed", error);
                showStatus("Send failed: " + shortError(error));
                runOnUiThread(() -> composer.setText(text));
            }
        });
    }

    private void showStatus(String value) {
        runOnUiThread(() -> status.setText(value));
    }

    private static String shortError(Exception error) {
        String message = error.getMessage();
        if (message == null || message.isEmpty()) {
            return error.getClass().getSimpleName();
        }
        return message.length() > 70 ? message.substring(0, 70) : message;
    }

    private String describeForLog(KeyEvent event) {
        InputDevice device = event.getDevice();
        return String.format(Locale.US,
                "action=%d key=%s keyCode=%d scanCode=%d device=%s",
                event.getAction(),
                KeyEvent.keyCodeToString(event.getKeyCode()),
                event.getKeyCode(),
                event.getScanCode(),
                device == null ? "unknown" : device.getName());
    }

    private TextView text(String value, float sizeSp, int color) {
        TextView view = new TextView(this);
        view.setText(value);
        view.setTextSize(sizeSp);
        view.setTextColor(color);
        return view;
    }

    private int dp(int value) {
        return Math.round(value * getResources().getDisplayMetrics().density);
    }
}
