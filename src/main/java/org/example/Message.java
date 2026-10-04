package org.example;

public class Message {
    private String sender;    // Отправитель
    private String recipient; // Получатель
    private String content;   // Само сообщение (текст)

    // Конструктор без параметров (нужен для сериализации в Kafka)
    public Message() {
    }

    // Конструктор со всеми параметрами
    public Message(String sender, String recipient, String content) {
        this.sender = sender;
        this.recipient = recipient;
        this.content = content;
    }

    // Геттеры и сеттеры
    public String getSender() {
        return sender;
    }

    public void setSender(String sender) {
        this.sender = sender;
    }

    public String getRecipient() {
        return recipient;
    }

    public void setRecipient(String recipient) {
        this.recipient = recipient;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }
}
