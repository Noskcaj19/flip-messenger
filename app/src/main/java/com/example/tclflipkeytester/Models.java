package com.example.tclflipkeytester;

import java.util.List;

final class Channel {
    final String id;
    final String qualifiedName;
    final String displayName;

    Channel(String id, String qualifiedName, String displayName) {
        this.id = id;
        this.qualifiedName = qualifiedName;
        this.displayName = displayName;
    }
}

final class Message {
    final String id;
    final String channelId;
    final String author;
    final String text;

    Message(String id, String channelId, String author, String text) {
        this.id = id;
        this.channelId = channelId;
        this.author = author;
        this.text = text;
    }
}

final class Snapshot {
    final String cursor;
    final List<Channel> channels;
    final List<Message> messages;

    Snapshot(String cursor, List<Channel> channels, List<Message> messages) {
        this.cursor = cursor;
        this.channels = channels;
        this.messages = messages;
    }
}

final class SyncResult {
    final String cursor;
    final List<Message> messages;

    SyncResult(String cursor, List<Message> messages) {
        this.cursor = cursor;
        this.messages = messages;
    }
}
