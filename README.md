# LoomMQ

<p align="center">
  <a href="https://github.com/j3rrryy/loommq/actions/workflows/main.yml">
    <img src="https://github.com/j3rrryy/loommq/actions/workflows/main.yml/badge.svg" alt="СI/CD">
  </a>
  <a href="https://codecov.io/gh/j3rrryy/loommq">
    <img src="https://codecov.io/gh/j3rrryy/loommq/graph/badge.svg?token=PFWPAH79T0" alt="Codecov">
  </a>
  <a href="https://www.oracle.com/java/technologies/downloads/#java25">
    <img src="https://img.shields.io/badge/Java-25-FFD64E.svg" alt="Java 25">
  </a>
  <a href="https://github.com/j3rrryy/loommq/blob/main/LICENSE">
    <img src="https://img.shields.io/badge/License-MIT-blue.svg" alt="MIT License">
  </a>
</p>

## :book: Ключевые особенности

**Протокол и маршрутизация:**

- Полная поддержка протокола MQTT 5
- Все уровни QoS (0, 1, 2)
- Поддержка MQTT wildcards (`#`, `+`) и Shared Subscriptions (балансировка нагрузки)
- Обработка Will-сообщений и Session Expiry Interval

**Надежность и хранение:**

- Персистентность сообщений через WAL (Write-Ahead Log) с восстановлением после сбоев
- Replay истории: перечитывание сообщений из WAL по запросу клиента через MQTT 5 User Properties
- Безопасная работа с памятью (Zero-copy `ByteBuf` и подсчет ссылок Netty)

**Безопасность:**

- Аутентификация пользователей на основе YAML-конфигурации с хэшированием паролей (Argon2)
- Гранулярный контроль доступа (ACL) на уровне топиков (publish/subscribe)
- Защита от брутфорса: Rate Limiting на CONNECT с блокировкой по IP

**Производительность:**

- Асинхронное ядро на Netty (Event Loop) с использованием Epoll/KQueue для максимальной пропускной способности
- Управление Backpressure: ограничение Receive Maximum и буферизация очередей per-client
- Использование ZGC с паузами < 1 мс

**Инфраструктура и Observability:**

- Экспорт метрик в формате Prometheus (`/metrics`)
- Выделенные эндпоинты для проб: Liveness (`/health/live`) и Readiness (`/health/ready`)
- Полностью контейнеризирован: Docker-образ, multi-stage сборка, non-root пользователь
- Готов к Cloud-Native: Helm-чарт и OCI-артефакты в GHCR

## :gear: Конфигурация

### `config.yaml`

```yaml
host: "0.0.0.0"               # Адрес, на котором слушает MQTT-порт
port: 1883                    # MQTT-порт
usersFile: "users.yaml"       # Путь к файлу с пользователями
walFile: "data/wal.log"       # Путь к Write-Ahead Log
rateLimit:
  maxAttempts: 5              # Максимум попыток CONNECT с одного IP
  windowSeconds: 60           # Окно в секундах для подсчета попыток
  banSeconds: 300             # Время бана IP после превышения лимита
```

По умолчанию читается `./config.yaml`, либо можно указать путь до файла через `--config ...`. Все поля
опциональны (отсутствующие имеют значения по умолчанию, как в примере).

### `users.yaml`

```yaml
users:
  - username: "admin"
    password: "my-secret-password"        # Открытый пароль или Argon2-хэш ($argon2id$...)
    permissions:
      - action: "all"
        topic: "#"

  - username: "billing"
    password: "$argon2id$v=19$m=19456,t=2,p=1$..."
    permissions:
      - action: "publish"
        topic: "orders/created"
      - action: "subscribe"
        topic: "billing/#"
      - action: "subscribe"
        topic: "events/+/billing"
```

| Поле     | Значения                                   | Описание                                        |
|----------|--------------------------------------------|-------------------------------------------------|
| `action` | `publish`, `subscribe`, `all`, `*` (`all`) | Действие, которое разрешено пользователю        |
| `topic`  | MQTT filter                                | Иерархия топиков, к которой применяется правило |

Если в `password` лежит **открытый текст**, брокер автоматически захэширует его через Argon2id при старте и будет
использовать хэш в рантайме. Если уже хэш — используется как есть.

### Переменные окружения

| Переменная                        | По умолчанию      | Описание                                                  |
|-----------------------------------|-------------------|-----------------------------------------------------------|
| `LOOMMQ_BOOTSTRAP_ADMIN_USERNAME` | `admin`           | Имя админа при Bootstrap                                  |
| `LOOMMQ_BOOTSTRAP_ADMIN_PASSWORD` | —                 | **Обязательно**, если `users.yaml` пустой или отсутствует |
| `JAVA_OPTS`                       | `-XX:+UseZGC ...` | JVM-флаги                                                 |

Bootstrap Admin активируется **только** если `users.yaml` отсутствует или пуст. В этом случае брокер создает
администратора из переменных окружения (с правами `*` на `#`) и логирует это как `WARN`.

### Helm: `values.yaml`

| Параметр                               | Описание                                                                                |
|----------------------------------------|-----------------------------------------------------------------------------------------|
| `replicaCount`                         | Количество реплик (рекомендуется `1` — брокер stateful, без поддержки распределенности) |
| `image.repository` / `tag`             | Docker-образ (по умолчанию `ghcr.io/j3rrryy/loommq:{{ .Chart.AppVersion }}`)            |
| `service.mqttPort` / `metricsPort`     | Порты в Service (1883 / 8080)                                                           |
| `persistence.enabled` / `size`         | PVC для WAL (по умолчанию `true` / `1Gi`)                                               |
| `bootstrapAdmin.username` / `password` | Данные для Bootstrap-админа (если не используется `existingSecret`)                     |
| `existingSecret`                       | Имя своего Secret с `users.yaml` (отключает Bootstrap)                                  |
| `existingConfigMap`                    | Имя своего ConfigMap с `config.yaml`                                                    |
| `config`                               | Inline-конфиг. Если задан — создается ConfigMap автоматически                           |
| `javaOpts`                             | JVM-флаги                                                                               |
| `resources`                            | Requests/Limits для пода                                                                |
| `podDisruptionBudget.enabled`          | Включить PDB (защита от случайной остановки)                                            |
| `networkPolicy.enabled`                | Включить NetworkPolicy (ingress 1883/8080, egress только DNS)                           |

## :computer: Что нужно для запуска

- Docker / Kubernetes + Helm
- Make
- Java 25 (опционально — для локальной разработки)

## :hammer_and_wrench: Начало работы

- **(Для локального запуска / Docker)** Скопируйте конфигурации из примеров и заполните их

  ```shell
  make setup
  ```

- **(Для Kubernetes)** При установке Helm-чарта выставите требуемые параметры в `values.yaml`

### :rocket: Запуск

- Локальное окружение

  ```shell
  make run
  ```

- Используя Docker

  ```shell
  make up
  ```

- Используя Kubernetes

  ```shell
  helm install loommq oci://ghcr.io/j3rrryy/charts/loommq --namespace loommq --create-namespace
  ```

### :x: Остановка

- Локальное окружение

  ```shell
  CTRL+C
  ```

- Используя Docker

  ```shell
  make down
  ```

- Используя Kubernetes

  ```shell
  helm uninstall loommq --namespace loommq
  ```
