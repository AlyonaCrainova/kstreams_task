package org.example;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.*;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

    public class FilterKafkaStreamsApp {
        // Конфигурация
        private static final String INPUT_TOPIC = "messages";
        private static final String OUTPUT_TOPIC = "filtered_messages";
        private static final String BLOCKED_USERS = "blocked_users";
        private static final String BLOCKED_USERS_STORE = "blocked_users_store";
        private static final String BANNED_WORDS = "banned_words";
        private static final String BANNED_WORDS_STORE = "banned_words_store";
        private static String bootstrapServers;

        public static void main(String[] args) {
            try {
                bootstrapServers = System.getenv("KAFKA_BOOTSTRAP_SERVERS");
                if (bootstrapServers == null || bootstrapServers.isEmpty()) {
                    System.err.println("Ошибка: переменная KAFKA_BOOTSTRAP_SERVERS не установлена!");
                    return;
                }
                // Создаем топики
                createTopics();

                // Создаем и запускаем Kafka Streams приложение
                KafkaStreams streams = buildStreamsApplication();
                final CountDownLatch latch = new CountDownLatch(1);

                // Обработка завершения работы
                Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                    System.out.println("Завершение работы приложения...");
                    streams.close();
                    latch.countDown();
                }));

                // Запускаем Kafka Streams
                streams.start();
                System.out.println("Приложение запущено");

                // Ждем, пока приложение полностью запустится
                waitUntilKafkaStreamsIsRunning(streams);

                System.out.println("Даем время на синхронизацию глобальных хранилищ...");
                TimeUnit.SECONDS.sleep(5);

                // Отправляем тестовые данные
                sendSampleBlockedUsersData();
                sendSampleBannedWordsData();
                sendSampleMessageData();

                // Даем время на обработку данных
                System.out.println("Ожидание обработки данных...");
                TimeUnit.SECONDS.sleep(5);

                // Ожидаем сигнала завершения
                latch.await();

            } catch (Exception e) {
                System.err.println("Ошибка при запуске приложения: " + e.getMessage());
                e.printStackTrace();
                System.exit(1);
            }
        }

        /**
         * Создает необходимые топики в Kafka
         */
        private static void createTopics() throws Exception {
            createTopic(BANNED_WORDS);
            createTopic(BLOCKED_USERS);
            createTopic(INPUT_TOPIC);
            createTopic(OUTPUT_TOPIC);
        }

        private static void createTopic(String topic) throws Exception {
            Properties adminProps = new Properties();
            adminProps.put("bootstrap.servers", bootstrapServers);

            try (AdminClient adminClient = AdminClient.create(adminProps)) {
                // Проверяем существование топика
                Set<String> existingTopics = adminClient.listTopics().names().get();

                if (!existingTopics.contains(topic)) {
                    // Создаем топик для данных о продажах
                    NewTopic newTopic = new NewTopic(topic, 1, (short) 1);

                    adminClient.createTopics(Collections.singletonList(newTopic)).all().get();
                    System.out.println("Топик " + topic + " создан");
                } else {
                    System.out.println("Топик " + topic + " уже существует");
                }
            }
        }


        /**
         * Создает и настраивает приложение Kafka Streams с использованием Processor API
         */
        private static KafkaStreams buildStreamsApplication() {
            // Настройка свойств Kafka Streams
            Properties props = new Properties();
            props.put(StreamsConfig.APPLICATION_ID_CONFIG, "filter-messages-processor");
            props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
            props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, MessageSerdes.class.getName());
            // Отключаем кеширование для наглядности
            props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

            // Создаем топологию с использованием Processor API

            Topology topology = new Topology();

            //Заблокированные пользователи
            StoreBuilder<KeyValueStore<String, String>> storeBuilder = Stores.keyValueStoreBuilder(
                    Stores.persistentKeyValueStore(BLOCKED_USERS_STORE),
                    Serdes.String(),
                    Serdes.String()
            ).withLoggingDisabled(); // Отключаем логирование для глобального хранилища

            topology.addGlobalStore(
                    storeBuilder,                   // Билдер хранилища
                    "BlockedUsersSource",             // Имя для источника глобального хранилища
                    Serdes.String().deserializer(), // Десериализатор ключа
                    Serdes.String().deserializer(), // Десериализатор значения
                    BLOCKED_USERS,                    // Входной топик для хранилища
                    "BlockedUsersProcessor",          // Имя процессора
                    () -> new org.apache.kafka.streams.processor.api.Processor<String, String, Void, Void>() {
                        private KeyValueStore<String, String> store;

                        @Override
                        public void init(org.apache.kafka.streams.processor.api.ProcessorContext<Void, Void> context) {
                            // Привязываем локальную переменную к созданному Store
                            this.store = context.getStateStore(BLOCKED_USERS_STORE);
                        }

                        @Override
                        public void process(org.apache.kafka.streams.processor.api.Record<String, String> record) {
                            // Логика обновления хранилища при изменении топика:
                            // Если пришел null (Tombstone) — удаляем слово, иначе — сохраняем в RocksDB
                            if (record.value() == null) {
                                store.delete(record.key());
                            } else {
                                store.put(record.key(), record.value());
                            }
                        }
                    } // Поставщик процессора
            );

            //Заблокированные слова
            storeBuilder = Stores.keyValueStoreBuilder(
                    Stores.persistentKeyValueStore(BANNED_WORDS_STORE),
                    Serdes.String(),
                    Serdes.String()
            ).withLoggingDisabled(); // Отключаем логирование для глобального хранилища
            topology.addGlobalStore(
                    storeBuilder,                   // Билдер хранилища
                    "BannedWordsSource",             // Имя для источника глобального хранилища
                    Serdes.String().deserializer(), // Десериализатор ключа
                    Serdes.String().deserializer(), // Десериализатор значения
                    BANNED_WORDS,                    // Входной топик для хранилища
                    "BannedWordsStateProcessor",          // Имя процессора
                    () -> new org.apache.kafka.streams.processor.api.Processor<String, String, Void, Void>() {
                        private KeyValueStore<String, String> store;

                        @Override
                        public void init(org.apache.kafka.streams.processor.api.ProcessorContext<Void, Void> context) {
                            // Привязываем локальную переменную к созданному Store
                            this.store = context.getStateStore(BANNED_WORDS_STORE);
                        }

                        @Override
                        public void process(org.apache.kafka.streams.processor.api.Record<String, String> record) {
                            // Логика обновления хранилища при изменении топика:
                            // Если пришел null (Tombstone) — удаляем слово, иначе — сохраняем в RocksDB
                            if (record.value() == null) {
                                store.delete(record.key());
                            } else {
                                store.put(record.key(), record.value());
                            }
                        }
                    } // Поставщик процессора
            );

            // Определяем источник данных (входной топик)
            topology.addSource("Source", INPUT_TOPIC);

            // Добавляем процессор, который будет обрабатывать сообщения
            topology.addProcessor("BlockUsersProcessor", BlockedUsersProcessor::new, "Source");
            topology.addProcessor("BannedWordsProcessor", BannedWordsProcessor::new, "BlockUsersProcessor");

            // Определяем выходной топик
            topology.addSink("Sink", OUTPUT_TOPIC, "BannedWordsProcessor");

            System.out.println("Топология: " + topology.describe());

            // Создаем приложение Kafka Streams
            return new KafkaStreams(topology, props);
        }

        /**
         * Процессор для фильтрации сообщений
         */
        public static class BlockedUsersProcessor implements Processor<String, Message, String, Message> {
            private ProcessorContext<String, Message> context;
            private KeyValueStore<String, String> blockedUsersStore;

            @Override
            public void init(ProcessorContext<String, Message> context) {
                this.context = context;
                // Получаем доступ к глобальному хранилищу
                this.blockedUsersStore = context.getStateStore(BLOCKED_USERS_STORE);
                System.out.println("Инициализирован процессор фильтрации заблокированных пользователей");
            }

            @Override
            public void process(Record<String, Message> record) {
                // Если пары отправитель:получаетель нет в списке блокировки, передаем сообщение дальше по топологии,
                // иначе - ничего не делаем, сообщение обработано
                if (blockedUsersStore.get( record.key() ) == null ){
                    context.forward(record);
                }
            }

            @Override
            public void close() {
                // Закрытие ресурсов (если необходимо)
            }
        }

        /**
         * Процессор для модерации сообщений: маскировка запрещенных слов
         */
        public static class BannedWordsProcessor implements Processor<String, Message, String, Message> {
            private ProcessorContext<String, Message> context;
            private KeyValueStore<String, String> bannedWordsStore;

            @Override
            public void init(ProcessorContext<String, Message> context) {
                this.context = context;
                // Получаем доступ к глобальному хранилищу
                this.bannedWordsStore = context.getStateStore(BANNED_WORDS_STORE);
                System.out.println("Инициализирован процессор модерации сообщений");
            }

            @Override
            public void process(Record<String, Message> record) {

                Message message = record.value();
                if (message == null || message.getContent() == null) {
                    context.forward(record); // Пропускаем пустые сообщения без изменений
                    return;
                }

                String content = message.getContent();

                // Разбираем сообщение на токены (слова)
                String[] tokens = content.split("\\s+");

                // Находим и маскируем забаненные токены, которые есть в этом сообщении
                Set<String> wordsToReplace = new HashSet<>();
                for (String token : tokens) {
                    // Чистим токен от знаков препинания и приводим к нижнему регистру. Предположим, что в списке только простые слова без тире.
                    String cleanToken = token.replaceAll("[^a-zA-Zа-яА-Я]", "").toLowerCase();
                    //если слово еще не обрабатывали, ищем в state store и маскируем в тексте сообщения, если нашли
                    if (wordsToReplace.contains(token) == false && bannedWordsStore.get(cleanToken) != null) {
                        wordsToReplace.add(token);
                        String replacement = "*".repeat(token.length());
                        content = content.replace(token, replacement);
                    }
                }

                // Обновляем контент в сообщении и пробрасываем измененную запись дальше по топологии
                message.setContent(content);
                context.forward(record.withValue(message));

            }

            @Override
            public void close() {
                // Закрытие ресурсов (если необходимо)
            }
        }

        /**
         * Отправляет тестовые данные
         */
        private static void sendSampleMessageData() {
            Properties producerProps = new Properties();
            producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers); // устанавливаем адрес для первого подключения
            producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, MessageSerializer.class);

            try (KafkaProducer<String, Message> producer = new KafkaProducer<>(producerProps)) {
                System.out.println("Отправка тестовых данных: сообщения...");

                producer.send(new ProducerRecord<>(INPUT_TOPIC, "user1:user2", new Message("user1", "user2", "Привет! Купи, пожалуйста, спелый ананас к ужину.")));
                producer.send(new ProducerRecord<>(INPUT_TOPIC, "ivan:anna", new Message("ivan", "anna", "На завтрак я обычно ем овсянку и спелый банан.")));
                producer.send(new ProducerRecord<>(INPUT_TOPIC, "user3:user4", new Message("user3", "user4", "Вчера в магазине была отличная скидка на сочный апельсин.")));
                producer.send(new ProducerRecord<>(INPUT_TOPIC, "alex:maria", new Message("alex", "maria", "Свежая груша отлично сочетается с этим сортом сыра.")));
                producer.send(new ProducerRecord<>(INPUT_TOPIC, "dmitry:elena", new Message("dmitry", "elena", "Добавь в корзину один лимон, он нужен для чая.")));
                producer.send(new ProducerRecord<>(INPUT_TOPIC, "user5:user6", new Message("user5", "user6", "Нам нужно купить виноград для праздничного стола.")));
                producer.send(new ProducerRecord<>(INPUT_TOPIC, "pavel:olga", new Message("pavel", "olga", "Этот персик выглядит очень сочным и сладким.")));
                producer.send(new ProducerRecord<>(INPUT_TOPIC, "user7:user8", new Message("user7", "user8", "Утром я выпил свежевыжатый сок, там был грейпфрут.")));

                producer.send(new ProducerRecord<>(INPUT_TOPIC, "user1:user2", new Message("user1", "user2", "Привет! Ты не забыл закрыть окно перед выходом из дома?")));
                producer.send(new ProducerRecord<>(INPUT_TOPIC, "alice:bob", new Message("alice", "bob", "Встретимся сегодня на нашей обычной остановке в шесть вечера.")));
                producer.send(new ProducerRecord<>(INPUT_TOPIC, "ivan:anna", new Message("ivan", "anna", "Погода сегодня просто отличная, давай прогуляемся по парку.")));
                producer.send(new ProducerRecord<>(INPUT_TOPIC, "user3:user4", new Message("user3", "user4", "Я закончил писать код для лабораторной работы по Кафке.")));
                producer.send(new ProducerRecord<>(INPUT_TOPIC, "alex:maria", new Message("alex", "maria", "Не забудь взять с собой ключи от квартиры, я буду поздно.")));
                producer.send(new ProducerRecord<>(INPUT_TOPIC, "dmitry:elena", new Message("dmitry", "elena", "Какой фильм мы будем смотреть в эти выходные?")));
                producer.send(new ProducerRecord<>(INPUT_TOPIC, "user5:user6", new Message("user5", "user6", "Поезд отправляется ровно через пятнадцать минут, поторопись.")));
                producer.send(new ProducerRecord<>(INPUT_TOPIC, "pavel:olga", new Message("pavel", "olga", "Я отправил все необходимые документы тебе на почту.")));
                producer.send(new ProducerRecord<>(INPUT_TOPIC, "sergey:igor", new Message("sergey", "igor", "Новый альбом этой группы звучит действительно круто.")));
                producer.send(new ProducerRecord<>(INPUT_TOPIC, "user7:user8", new Message("user7", "user8", "Завтра утром обещают сильный дождь, возьми зонтик.")));

                producer.flush();
                System.out.println("Тестовые данные успешно отправлены");

            } catch (Exception e) {
                System.err.println("Ошибка при отправке тестовых данных: " + e.getMessage());
                e.printStackTrace();
            }
        }

        private static void sendSampleBlockedUsersData() {
            Properties producerProps = new Properties();
            producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);// устанавливаем адрес для первого подключения
            producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

            try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
                System.out.println("Отправка тестовых данных: блокировки пользователей...");

                producer.send(new ProducerRecord<>(BLOCKED_USERS, "user1:user2", ""));
                producer.send(new ProducerRecord<>(BLOCKED_USERS, "alice:bob", ""));

                producer.flush();
                System.out.println("Тестовые данные успешно отправлены");

            } catch (Exception e) {
                System.err.println("Ошибка при отправке тестовых данных: " + e.getMessage());
                e.printStackTrace();
            }
        }

        private static void sendSampleBannedWordsData() {
            Properties producerProps = new Properties();
            producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

            try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
                System.out.println("Отправка тестовых данных: забаненные слова...");

                producer.send(new ProducerRecord<>(BANNED_WORDS, "ананас", ""));
                producer.send(new ProducerRecord<>(BANNED_WORDS, "банан", ""));
                producer.send(new ProducerRecord<>(BANNED_WORDS, "апельсин", ""));
                producer.send(new ProducerRecord<>(BANNED_WORDS, "груша", ""));
                producer.send(new ProducerRecord<>(BANNED_WORDS, "лимон", ""));
                producer.send(new ProducerRecord<>(BANNED_WORDS, "виноград", ""));
                producer.send(new ProducerRecord<>(BANNED_WORDS, "персик", ""));
                producer.send(new ProducerRecord<>(BANNED_WORDS, "грейпфрут", ""));


                producer.flush();
                System.out.println("Тестовые данные успешно отправлены");

            } catch (Exception e) {
                System.err.println("Ошибка при отправке тестовых данных: " + e.getMessage());
                e.printStackTrace();
            }
        }

        /**
         * Ожидает, пока Kafka Streams перейдет в состояние RUNNING
         */
        private static void waitUntilKafkaStreamsIsRunning(KafkaStreams streams) throws Exception {
            int maxRetries = 10;
            int retryIntervalMs = 1000;
            int attempt = 0;

            while (attempt < maxRetries) {
                if (streams.state() == KafkaStreams.State.RUNNING) {
                    System.out.println("Kafka Streams успешно запущен");
                    return;
                }

                System.out.println("Ожидание запуска Kafka Streams... Текущее состояние: " + streams.state());
                Thread.sleep(retryIntervalMs);
                attempt++;
            }

            throw new RuntimeException("Превышено время ожидания запуска Kafka Streams");
        }

    }

