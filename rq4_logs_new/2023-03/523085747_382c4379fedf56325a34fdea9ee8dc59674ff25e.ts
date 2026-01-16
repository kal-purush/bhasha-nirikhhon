import { getDockerComposeContents } from "./get-docker-compose-contents";

describe("getDockerComposeContents", () => {
  it("should return docker compose contents", () => {
    const result = getDockerComposeContents("mock service name", "postgres");

    expect(result).toEqual(`version: '3.9'
services:
  mock service name:
    container_name: mock service name
    # command: yarn docker-develop
    restart: always
    build: .
    ports:
      - \${PORT}:\${PORT}
    env_file:
      - .env
    depends_on:
      - mock service name_postgres
    volumes:
      - .:/app
      - ./src/public:/app/dist/public
      - ./src:/app/src
      - ./dist:/app/dist


  mock service name_pgadmin:
    container_name: mock service name_pgadmin
    restart: always
    image: dpage/pgadmin4
    env_file:
      - .env
    ports:
      - \${PGADMIN_LISTEN_PORT}:\${PGADMIN_LISTEN_PORT}
    depends_on:
      - mock service name_postgres

  mock service name_postgres:
    container_name: mock service name_postgres
    image: postgres
    ports:
      - \${DB_PORT}:5432
    volumes:
      - mock service name:/data/db_mock service name_postgres
    environment:
      - POSTGRES_PASSWORD=\${DB_PASSWORD}
      - POSTGRES_DB=\${DB_NAME}

volumes:
  mock service name: {}
`);
  });
});