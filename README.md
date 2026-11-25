# NetPulse

Monitor de rede web no estilo **The Dude (MikroTik)**: você cadastra os IPs dos equipamentos, organiza tudo em mapas de topologia e acompanha em tempo real quem está online, lento ou offline, com **ping ICMP real**.

## O que faz

- Mapa de topologia interativo (arrastar, zoom, dashboard em tela cheia, tema claro/escuro)
- **Vários mapas**, cada um com seus próprios equipamentos, posições e zoom
- Ping real a cada 5 s, com latência, disponibilidade (24 h), histórico e log de eventos
- Status: `Online`, `Lento`, `Offline` e `Sem rota` (quando o equipamento "pai" caiu)
- Cadastro e **edição** de equipamentos: Servidor, Roteador, Switch, OLT, DVR, Retificadora, Régua volt, Access point, Câmera, Totem de segurança e Computador
- **Importar / exportar JSON** (exemplo para download, backup e migração)
- Login com usuários e perfis: **Administrador** (gerencia tudo) e **Visualizador** (só vê)

## Tecnologias

| Camada | Tecnologia |
|---|---|
| Backend | Python 3.12, FastAPI, Uvicorn |
| Banco de dados | PostgreSQL 16 |
| Acesso ao banco | psycopg 3 (com pool de conexões) |
| Autenticação | JWT (PyJWT), senhas com bcrypt |
| Ping | `ping` do pacote iputils (ICMP real) |
| Frontend | HTML, CSS e JavaScript puros (SVG), sem build |
| Execução | Docker e Docker Compose |

## Estrutura

```
.
├── backend/
│   ├── main.py            # API, autenticação, poller de ping, migração do banco
│   └── requirements.txt
├── frontend/
│   └── index.html         # interface completa (servida pelo backend)
├── Dockerfile
├── docker-compose.yml     # serviços: app (FastAPI) e db (PostgreSQL)
├── .env.example
└── README.md
```

## Requisitos

No servidor (Debian 12) você só precisa instalar:

| Programa | Para quê |
|---|---|
| Docker Engine | executar os containers |
| Docker Compose (plugin) | subir app + banco juntos |
| Git | baixar o projeto |
| Caddy ou Nginx *(opcional)* | HTTPS com domínio |
| UFW *(opcional)* | firewall |

Python e PostgreSQL **rodam dentro dos containers**, não precisam ser instalados no servidor.

Recursos mínimos sugeridos: 1 vCPU, 1 GB de RAM e 5 GB de disco.

## Instalação no Debian 12

### 1. Atualizar o sistema e instalar utilitários

```bash
sudo apt update && sudo apt upgrade -y
sudo apt install -y ca-certificates curl git openssl
```

### 2. Instalar o Docker e o Compose

```bash
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/debian/gpg -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc

echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] \
https://download.docker.com/linux/debian $(. /etc/os-release && echo $VERSION_CODENAME) stable" \
 | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

sudo apt update
sudo apt install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
```

Teste:

```bash
sudo docker run --rm hello-world
docker compose version
```

*(Opcional)* usar o Docker sem `sudo`: `sudo usermod -aG docker $USER` e depois sair e entrar de novo na sessão.

### 3. Baixar o projeto

```bash
cd /opt
sudo git clone https://github.com/SEU_USUARIO/netpulse.git
sudo chown -R $USER:$USER /opt/netpulse
cd /opt/netpulse
```

### 4. Configurar o `.env`

```bash
cp .env.example .env
```

Gere senhas fortes e cole no `.env`:

```bash
openssl rand -hex 16   # use para DB_PASSWORD e ADMIN_PASSWORD
openssl rand -hex 32   # use para SECRET_KEY
```

```bash
nano .env
```

| Variável | Descrição | Padrão |
|---|---|---|
| `PORT` | porta publicada no servidor | `8080` |
| `DB_PASSWORD` | senha do PostgreSQL (evite caracteres especiais) | `netpulse` |
| `SECRET_KEY` | chave de assinatura dos tokens de login | `troque-esta-chave` |
| `ADMIN_USER` | usuário administrador inicial | `admin` |
| `ADMIN_PASSWORD` | senha do administrador inicial | `admin123` |
| `POLL_INTERVAL` | segundos entre as rodadas de ping | `5` |
| `PING_TIMEOUT` | tempo limite de cada ping (s) | `2` |
| `SLOW_MS` | acima disso o equipamento fica "Lento" | `100` |
| `FAIL_THRESHOLD` | falhas seguidas para marcar "Offline" | `2` |

> O administrador é criado **apenas na primeira subida**, quando o banco ainda não tem usuários. Depois disso, altere senhas pela própria interface.

### 5. Subir o sistema

```bash
docker compose up -d --build
docker compose ps
docker compose logs -f app
```

Acesse `http://IP_DO_SERVIDOR:8080` e entre com o usuário e a senha definidos no `.env`.

### 6. Firewall (recomendado)

```bash
sudo apt install -y ufw
sudo ufw allow OpenSSH
sudo ufw allow 80/tcp
sudo ufw allow 443/tcp
sudo ufw enable
```

> **Atenção:** o Docker publica portas direto no iptables e ignora o UFW. Se for usar proxy com HTTPS, no `docker-compose.yml` troque a porta do serviço `app` para
> `"127.0.0.1:${PORT:-8080}:8000"` e rode `docker compose up -d` de novo. Assim o sistema só fica acessível pelo proxy.

### 7. HTTPS com domínio (opcional, com Caddy)

Aponte o DNS do domínio para o IP do servidor e rode:

```bash
sudo apt install -y caddy
sudo nano /etc/caddy/Caddyfile
```

```
monitor.seudominio.com {
    reverse_proxy 127.0.0.1:8080
}
```

```bash
sudo systemctl reload caddy
```

O Caddy emite e renova o certificado automaticamente. Se o pacote não estiver disponível, siga as instruções oficiais em https://caddyserver.com/docs/install.

## Sobre o ping e a rede

- O container usa a capacidade `NET_RAW` para enviar ICMP (já configurada no `docker-compose.yml`).
- Ele precisa **alcançar os IPs monitorados**. Se eles estiverem em outra rede, VPN ou VLAN que o container não enxerga, adicione `network_mode: host` ao serviço `app` (e remova a seção `ports`; a porta passa a ser `8000`).
- Equipamentos que bloqueiam ICMP aparecem como **Offline**.

## Operação

```bash
# logs
docker compose logs -f app

# atualizar para uma nova versão
git pull && docker compose up -d --build

# parar / iniciar
docker compose down
docker compose up -d

# backup do banco
docker compose exec -T db pg_dump -U netpulse netpulse > backup_$(date +%F).sql

# restaurar backup
cat backup.sql | docker compose exec -T db psql -U netpulse netpulse
```

Os dados ficam no volume Docker `pgdata` e sobrevivem a reinícios e atualizações. **Não use** `docker compose down -v`, pois isso apaga o banco.

## Importar / exportar JSON

Na barra lateral (administradores), seção **Importar / exportar**:

- **Baixar exemplo JSON**: arquivo modelo para preencher
- **Exportar tudo**: todos os mapas e equipamentos (backup / migração)
- **Importar JSON**: adiciona os equipamentos; IPs já existentes no mapa são ignorados

```json
{
  "maps": [
    {
      "name": "Matriz",
      "devices": [
        { "name": "Gateway", "ip": "192.168.10.1", "type": "router", "parent": null, "x": 500, "y": 300 },
        { "name": "DVR portaria", "ip": "192.168.10.20", "type": "dvr", "parent": "192.168.10.1" }
      ]
    }
  ]
}
```

- `type`: `server`, `router`, `switch`, `olt`, `dvr`, `retificadora`, `regua`, `ap`, `camera`, `totem`, `pc`
- `parent`: IP do equipamento pai no mesmo mapa, ou `null`
- `x` e `y` são opcionais; sem eles o mapa é organizado automaticamente

## Segurança

- Troque `SECRET_KEY`, `DB_PASSWORD` e a senha do administrador antes de usar em produção
- Use HTTPS (proxy reverso) quando o acesso for pela internet
- O login bloqueia o usuário por 5 minutos após 8 tentativas erradas
- Faça backup do banco com frequência

## Documentação da API

A documentação interativa (Swagger) fica em `/docs`. Os endpoints de dados exigem login (token `Bearer`).

## Licença

Defina a licença do seu projeto (por exemplo, MIT) neste espaço.
