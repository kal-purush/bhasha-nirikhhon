import { Pool, PoolClient } from "pg";
import {
  EditableProviderTypes,
  EmailCreateBody,
  EmailProvider,
  ProvidersList,
  ProvidersListItem,
} from "../../../types/providers.js";
import { PaginationParams } from "../../../types/schemaDefinitions.js";
import { SpecificProvider } from "../provider-factory.js";
import { httpErrors } from "@fastify/sensible";

type ListProvidersQueryResult = ProvidersListItem & {
  count: number;
};

export class EmailSpecificProvider implements SpecificProvider<EmailProvider> {
  readonly providerType: EditableProviderTypes;
  constructor(
    readonly pool: Pool,
    readonly organisationId: string,
  ) {
    this.providerType = "email";
  }

  async get(params: { providerId: string }): Promise<EmailProvider> {
    let provider: EmailProvider | undefined = undefined;
    try {
      const queryResult = await this.pool.query<EmailProvider>(
        `
               SELECT 
                   id,
                   'email' as "type",
                   provider_name as "providerName",
                   COALESCE(is_primary, false) as "isPrimary",
                   smtp_host as "smtpHost",
                   smtp_port as "smtpPort",
                   username,
                   pw as "password",
                   COALESCE(throttle_ms, 0) as "throttle",
                   from_address as "fromAddress",
                   is_ssl as "ssl"
               FROM email_providers
               WHERE organisation_id = $1 AND id = $2
               AND deleted_at is null
               ORDER BY provider_name
           `,
        [this.organisationId, params.providerId],
      );

      provider = queryResult.rows.at(0);
    } catch (error) {
      throw httpErrors.createError(500, "failed to query email provider", {
        parent: error,
      });
    }

    if (!provider) {
      throw httpErrors.notFound("failed to find email provider");
    }

    return provider;
  }

  async getPrimary(): Promise<EmailProvider> {
    let provider: EmailProvider | undefined = undefined;
    try {
      const queryResult = await this.pool.query<EmailProvider>(
        `
                SELECT 
                    id,
                    'email' as "type",
                    provider_name as "providerName",
                    is_primary as "isPrimary",
                    smtp_host as "smtpHost",
                    smtp_port as "smtpPort",
                    username,
                    pw as "password",
                    COALESCE(throttle_ms, 0) as "throttle",
                    from_address as "fromAddress",
                    is_ssl as "ssl"
                FROM email_providers
                WHERE organisation_id = $1 AND is_primary = true
                AND deleted_at is null
                ORDER BY provider_name
            `,
        [this.organisationId],
      );

      provider = queryResult.rows.at(0);
    } catch (error) {
      throw httpErrors.createError(
        500,
        "failed to query primary email provider",
        {
          parent: error,
        },
      );
    }

    if (!provider) {
      throw httpErrors.notFound("failed to find primary email provider");
    }

    return provider;
  }

  async delete(params: { providerId: string }): Promise<void> {
    let deleted = 0;
    try {
      const deleteQueryResult = await this.pool.query(
        `
        UPDATE email_providers 
            SET deleted_at = now()
            WHERE id = $1 AND organisation_id = $2
            RETURNING 1
      `,
        [params.providerId, this.organisationId],
      );

      deleted = deleteQueryResult.rowCount || 0;
    } catch (error) {
      throw httpErrors.createError(500, "failed delete query", {
        parent: error,
      });
    }

    if (deleted === 0) {
      throw httpErrors.badRequest("no provider found");
    }
  }

  async create(params: { inputBody: EmailCreateBody }): Promise<string> {
    const { inputBody } = params;
    await this.ensureEmailProviderDoesntExist({
      inputBody,
      organisationId: this.organisationId,
    });

    const client = await this.pool.connect();
    try {
      client.query("BEGIN");

      const isPrimary = await this.needToSetProviderAsPrimary({
        client,
        organisationId: this.organisationId,
        setAsPrimary: inputBody.isPrimary,
        tableName: "email_providers",
      });

      if (isPrimary) {
        await client.query(
          `
          UPDATE email_providers
          SET is_primary = null
          WHERE organisation_id = $1
        `,
          [this.organisationId],
        );
      }

      const queryResult = await client.query<{ providerId: string }>(
        `
          INSERT INTO email_providers(provider_name, smtp_host, smtp_port, username, pw, from_address, throttle_ms, is_ssl, organisation_id, is_primary)
          VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
          RETURNING id as "providerId"
          `,
        [
          inputBody.providerName,
          inputBody.smtpHost,
          inputBody.smtpPort,
          inputBody.username,
          inputBody.password,
          inputBody.fromAddress,
          inputBody.throttle,
          inputBody.ssl,
          this.organisationId,
          isPrimary,
        ],
      );

      const providerId = queryResult.rows.at(0)?.providerId;
      if (!providerId) {
        throw httpErrors.internalServerError("no record has been inserted");
      }

      await client.query("COMMIT");

      return providerId;
    } catch (error) {
      await client.query("ROLLBACK");
      throw httpErrors.createError(500, "failed to insert email provider", {
        parent: error,
      });
    } finally {
      client.release();
    }
  }

  async update(params: { inputBody: EmailProvider }): Promise<void> {
    const { inputBody } = params;
    await this.ensureEmailProviderDoesntExist({
      inputBody,
      organisationId: this.organisationId,
      providerIdToIgnore: params.inputBody.id,
    });

    const client = await this.pool.connect();

    try {
      client.query("BEGIN");
      const isPrimary = await this.needToSetProviderAsPrimary({
        client,
        organisationId: this.organisationId,
        setAsPrimary: inputBody.isPrimary,
        providerIdToIgnore: inputBody.id,
        tableName: "email_providers",
      });
      if (isPrimary) {
        await client.query(
          `
          UPDATE email_providers SET is_primary = null
          WHERE organisation_id = $1
          `,
          [this.organisationId],
        );
      }

      await client.query(
        `
            UPDATE email_providers SET 
              provider_name = $1, 
              smtp_host = $2,
              smtp_port = $3,
              username = $4,
              pw = $5,
              from_address = $6,
              throttle_ms = $7,
              is_ssl = $8,
              is_primary = $9
            WHERE id = $10 AND organisation_id = $11
          `,
        [
          inputBody.providerName,
          inputBody.smtpHost,
          inputBody.smtpPort,
          inputBody.username,
          inputBody.password,
          inputBody.fromAddress,
          inputBody.throttle,
          inputBody.ssl,
          isPrimary,
          inputBody.id,
          this.organisationId,
        ],
      );

      client.query("COMMIT");
    } catch (error) {
      client.query("ROLLBACK");
      throw httpErrors.createError(500, "failed to update email provider", {
        parent: error,
      });
    } finally {
      client.release();
    }
  }

  async list(params: {
    isPrimary: boolean | undefined;
    pagination: Required<PaginationParams>;
  }): Promise<{ data: ProvidersList; totalCount: number }> {
    let isPrimaryWhereClause = "";
    if (params.isPrimary !== undefined) {
      isPrimaryWhereClause = params.isPrimary
        ? "AND is_primary = true"
        : "AND is_primary != true";
    }
    try {
      const result = await this.pool.query<ListProvidersQueryResult>(
        `
            WITH count_selection as(
                SELECT count(*) FROM email_providers
                WHERE organisation_id = $1
                AND deleted_at is null
                ${isPrimaryWhereClause}
            )
            SELECT
                id,
                provider_name as "providerName",
                is_primary as "isPrimary",
                'email' as "type",
                (SELECT count FROM count_selection) as "count"
            FROM email_providers
            WHERE organisation_id = $1
            AND deleted_at is null 
            ${isPrimaryWhereClause}
            ORDER BY provider_name
            LIMIT $2
            OFFSET $3
        `,
        [
          this.organisationId,
          params.pagination.limit,
          params.pagination.offset,
        ],
      );

      const totalCount = result.rowCount === 0 ? 0 : result.rows.at(0)?.count;
      if (!totalCount) {
        return { data: [], totalCount: 0 };
      }
      // removing count field from output
      const output = result.rows.map(
        ({ count, ...otherFields }) => otherFields,
      );

      return { data: output, totalCount };
    } catch (error) {
      throw httpErrors.createError(500, "failed to query email providers", {
        parent: error,
      });
    }
  }

  private async ensureEmailProviderDoesntExist(params: {
    inputBody: EmailCreateBody;
    organisationId: string;
    providerIdToIgnore?: string;
  }): Promise<void> {
    const values: string[] = [
      params.organisationId,
      params.inputBody.fromAddress,
      params.inputBody.providerName,
    ];
    let templateIdToIgnoreWhere = "";
    if (params.providerIdToIgnore) {
      templateIdToIgnoreWhere = " AND id != $4 ";
      values.push(params.providerIdToIgnore);
    }

    const duplicationQueryResult = await this.pool.query<{
      exists: boolean;
    }>(
      `
      SELECT exists(
        SELECT * from email_providers
        WHERE organisation_id = $1 
        AND (lower(from_address) = lower($2) OR lower(provider_name) = lower($3))
        ${templateIdToIgnoreWhere}
      )
    `,
      values,
    );

    const addressExists = Boolean(duplicationQueryResult.rows.at(0)?.exists);

    if (addressExists) {
      throw httpErrors.createError(
        422,
        "provider from address or name already exists",
        {
          validation: [
            {
              fieldName: "fromAddress",
              message: "alreadyInUse",
              validationRule: "already-in-use",
            },
          ],
        },
      );
    }
  }

  private async needToSetProviderAsPrimary(params: {
    client: PoolClient;
    organisationId: string;
    providerIdToIgnore?: string;
    setAsPrimary: boolean | null;
    tableName: "email_providers";
  }): Promise<boolean | null> {
    if (params.setAsPrimary) {
      return true;
    }
    const values = [params.organisationId];
    let idWhereClause = "";
    if (params.providerIdToIgnore) {
      idWhereClause = " AND id != $2";
      values.push(params.providerIdToIgnore);
    }
    const otherProviders = await params.client.query<{ id: string }>(
      `
              SELECT id FROM ${params.tableName}
              WHERE organisation_id = $1
              AND deleted_at is null
              ${idWhereClause}
              LIMIT 1
            `,
      values,
    );

    return otherProviders.rowCount === 0 || null;
  }
}