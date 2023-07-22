import axios, { AxiosResponse } from "axios";
import pg, { Pool } from "pg";
import { isEqual } from 'lodash';


interface WebhookSpecification {
  options: {
    filters: {
      dataTypes: string[];
   
    };
  };
}


async function fetchTables(token: string, baseID: string): Promise<any[]> {
  try {
    const response: AxiosResponse = await axios.get(
      `https://api.airtable.com/v0/meta/bases/${baseID}/tables`,
      {
        headers: { Authorization: `Bearer ${token}` },
      }
    );
    return response.data.tables || [];
  } catch (error: any) {
    console.error("Error fetching Tables:", error.message);
    return [];
  }
}

async function fetchTableRecords(
  token: string,
  baseID: string,
  tableName: string
) {
  try {
    const response: AxiosResponse = await axios.get(
      `https://api.airtable.com/v0/${baseID}/${tableName}`,
      {
        headers: { Authorization: `Bearer ${token}` },
      }
    );
    return response.data.records || [];
  } catch (error: any) {
    console.log("Error fetching Records:", error.message);
  }
}

async function createPostgresTable(
  pgURL: string,
  tables:any[],
  tableName: string
) {
  
  if (!tables) {
    console.log("Error fetching tables");
    return;
  }

  const table = tables.find((t: any) => t.name === tableName);
  const pool = new Pool({ connectionString: pgURL, ssl: true });

  const client = await pool.connect();
  console.log("Table created successfully");

  try {
    const fields = table?.fields;
    if (!fields || fields.length === 0) {
      console.log("No fields found in the table.");
      return;
    }

    const columnDefinitions = fields.map((field: any) => {
      const columnName =
        field.name === "Unit cost" ? `"${field.name}"` : `"${field.name}"`;
      return `${columnName} ${mapAirtableTypeToPostgresType(field.type)}`;
    });

    const createTableQuery = `CREATE TABLE IF NOT EXISTS "${tableName}" (${columnDefinitions.join(
      ", "
    )})`;

    console.log("Create table query:", createTableQuery);

    await client.query(createTableQuery);
    console.log("Table Created Successfully");
  } catch (error) {
    console.error("Error creating PostgreSQL table:", error);
  } finally {
    client.release();
  }
}

async function updatePostgresTable(pgURL: string, payload: any,token:string,baseID:string) {
  
  const pool = new Pool({ connectionString: pgURL, ssl: true });
    const client = await pool.connect();
  try {
    

    const lastPayload = payload.payloads[payload.payloads.length - 1];
    const changedTablesById = lastPayload.changedTablesById;
    console.log(lastPayload)


    if (changedTablesById) {
      const tableId = Object.keys(changedTablesById)[0];
      const changedRecordsById = changedTablesById[tableId].changedRecordsById;
      const tables = await fetchTables(token,baseID)

      const table = tables.find((t)=>t.id === tableId)

      if (changedRecordsById) {
        const recordId = Object.keys(changedRecordsById)[0];
        const changedFieldsById = changedRecordsById[recordId].changedFieldsById;


        if (changedFieldsById) {
          const fieldId = Object.keys(changedFieldsById)[0];
          const fieldName = changedFieldsById[fieldId].current.name;
          const newValue = changedFieldsById[fieldId].current.newValue;

          const updateFieldValueQuery = `
            UPDATE "${table.name}"
            SET "${fieldName}" = '${newValue}'
            WHERE "id" = '${recordId}';
          `;

          await client.query(updateFieldValueQuery);
          console.log(`Field "${fieldName}" updated to: ${newValue}`);
        }
      }


      if (changedRecordsById) {
        const recordId = Object.keys(changedRecordsById)[0];
        const currentValues = changedRecordsById[recordId].current.cellValuesByFieldId;

        for (const fieldId in currentValues) {
          const newFieldName = currentValues[fieldId];

          const updateFieldNameQuery = `
            ALTER TABLE "${table.name}"
            RENAME COLUMN "${fieldId}" TO "${newFieldName}";
          `;

          await client.query(updateFieldNameQuery);
          console.log(`Field name updated to: ${newFieldName}`);
        }
      }

      if (changedTablesById[tableId].changedMetadata) {
        const newTableName = changedTablesById[tableId].changedMetadata.current.name;
        const oldTableName = changedTablesById[tableId].changedMetadata.previous.name;

        const updateTableNameQuery = `
          ALTER TABLE "${oldTableName}"
          RENAME TO "${newTableName}";
        `;

        await client.query(updateTableNameQuery);
        console.log(`Table name updated to: ${newTableName}`);
      }
    }



  } catch (error) {
    console.error("Error updating PostgreSQL table:", error);
  }finally{
  client.release()
  }
}


async function insertRecordsToPostgres(
  token: string,
  baseId: string,
  tableName: string,
  pgURL: string
) {
  
  const pool = new Pool({ connectionString: pgURL, ssl: true });
  const client = await pool.connect();

  try {
    const tableRecords = await fetchTableRecords(token,baseId,tableName);
    if (tableRecords.length === 0) {
      console.log("No records to insert.");
      return;
    }

    for (const record of tableRecords) {
      const fieldNames = Object.keys(record.fields);
      const fieldValues = Object.values(record.fields);

      const formattedValues = fieldValues.map((value, index) => {
        const fieldName = fieldNames[index];
        if (Array.isArray(value)) {
          const formattedArrayValues = value.map((v) => {
            if (typeof v === "object" && v !== null && "url" in v) {
              return v.url;
            } else {
              return JSON.stringify(v);
            }
          });

          return `'{${formattedArrayValues.join(",")}}'`;
        } else if (typeof value === "string") {
          return `'${value.replace(/'/g, "''")}'`;
        } else {
          return value;
        }
      });

      const insertQuery = `
        INSERT INTO "${tableName}" (${fieldNames
        .map((name) => `"${name}"`)
        .join(", ")})
        VALUES (${formattedValues.join(", ")})
      `;

      await client.query(insertQuery);
    }

    console.log("Records inserted successfully.");
  } catch (error) {
    console.error("Error inserting records:", error);
  } finally {
    client.release();
  }
}
async function fetchWebhooks(baseID: string, token: string): Promise<any> {
  const response: AxiosResponse<any> = await axios.get(
    `https://api.airtable.com/v0/bases/${baseID}/webhooks`,
    {
      headers: {
        Authorization: `Bearer ${token}`,
      },
    }
  );

  return response.data;
}

async function refreshWebhook(
  baseID: string,
  token: string,
  webhookID: string
): Promise<any> {
  const response: AxiosResponse<any> = await axios.post(
    `https://api.airtable.com/v0/bases/${baseID}/webhooks/${webhookID}/refresh`,
    {},
    {
      headers: {
        Authorization: `Bearer ${token}`,
      },
    }
  );

  return response.data;
}

async function createWebhook(
  baseID: string,
  apiKey: string,
  notificationURL: string,
  specification: WebhookSpecification
): Promise<any> {
  try {
    const response: AxiosResponse<any> = await axios.post(
      `https://api.airtable.com/v0/bases/${baseID}/webhooks`,
      {
        notificationUrl: notificationURL,
        specification,
      },
      {
        headers: {
          Authorization: `Bearer ${apiKey}`,
          "Content-Type": "application/json",
        },
      }
    );

    return response.data;
  } catch (error) {
    if (axios.isAxiosError(error)) {
      if (error.response) {
        console.error("Error status:", error.response.status);
        console.error("Error data:", error.response.data);
      } else {
        console.error("Error message:", error.message);
      }
    } else {
      console.error("Non-Axios error occurred:", error);
    }
  }
}
async function fetchWebhookPayload(baseID: string, token: string, webhookID: string): Promise<any[]> {
  try {
    const response: AxiosResponse = await axios.get(
      `https://api.airtable.com/v0/bases/${baseID}/webhooks/${webhookID}/payloads`,
      {
        headers: { Authorization: `Bearer ${token}` },
      }
    );

    return response.data.payloads || [];
  } catch (error: any) {
    console.error("Error fetching webhook payload:", error.message);
    return [];
  }
}

let uniqueWebhookCount = 0;
  
  async function manageWebhooks(
    webhookSpecifications: WebhookSpecification[],
    notificationURL: string,
    token: string,
    baseID: string
  ): Promise<any> {
    try {
      const webhooks = await fetchWebhooks(baseID, token);
      const webhookResults: any[] = [];
      const existingWebhookSpecifications = new Set<string>();
  
      for (const webhook of webhooks.webhooks || []) {
        existingWebhookSpecifications.add(JSON.stringify(webhook.specification));
      }
  
      for (const webhookSpecification of webhookSpecifications) {
        if (uniqueWebhookCount > 3) {
          console.log("Reached the maximum limit of three webhooks. Skipping creation and refreshing.");
          await refreshExistingWebhooks(webhooks, webhookSpecifications, baseID, token);
          break;
        }
  
        if (existingWebhookSpecifications.size <= 3) {
          if (!existingWebhookSpecifications.has(JSON.stringify(webhookSpecification))) {
            const createRes = await createWebhook(
              baseID,
              token,
              notificationURL,
              webhookSpecification
            );
            webhookResults.push(createRes);
            existingWebhookSpecifications.add(JSON.stringify(webhookSpecification));
            uniqueWebhookCount++;
          } else {
            console.log(`Webhook with specification ${JSON.stringify(webhookSpecification)} already exists. Skipping creation and refreshing.`);
            await refreshExistingWebhooks(webhooks, webhookSpecifications, baseID, token);
          }
        } else {
          console.log(`Reached the maximum limit of three webhooks with different specifications. Skipping creation and refreshing for specification ${JSON.stringify(webhookSpecification)}.`);
          await refreshExistingWebhooks(webhooks, webhookSpecifications, baseID, token);
        }
      }
  
      return webhookResults;
    } catch (error: any) {
      console.error("Error managing webhooks:", error.message);
      throw error;
    }
  }
  
  async function refreshExistingWebhooks(
    existingWebhooks: any,
    webhookSpecifications: WebhookSpecification[],
    baseID: string,
    token: string
  ): Promise<void> {
    for (const webhookSpecification of webhookSpecifications) {
      const existingWebhook = existingWebhooks.webhooks?.find((webhook: any) =>
        isEqual(webhook.specification, webhookSpecification)
      );
  
      if (existingWebhook) {
      
        const refreshRes = await refreshWebhook(baseID, token, existingWebhook.id);
        console.log(`Webhook with specification ${JSON.stringify(webhookSpecification)} refreshed.`);
      }
    }
  }


function mapAirtableTypeToPostgresType(airtableType: string): string {
  switch (airtableType) {
    case "text":
    case "multilineText":
    case "singleLineText":
      return "TEXT";
    case "number":
      return "NUMERIC";
    case "multipleSelects":
      return "TEXT[]";
    case "date":
      return "DATE";
    case "checkbox":
      return "BOOLEAN";
    case "multipleAttachments":
      return "TEXT[]";
    case "currency":
      return "NUMERIC";
    case "rollup":
      return "NUMERIC";
    case "multipleRecordLinks":
      return "TEXT[]";
    default:
      return "TEXT";
  }
}

async function main() {
  const baseID = "appff8HJFMBuoXgwd";
  const tName = "Furniture";
  const token =
    "patwI1yOnv2nlWULt.2d3d51fcb8653f64bac88467c64e063ae1eafbfdfd67823f60a9a0d3be13183c";

  const pgUrl =
    "postgres://latipovabdukhamid:dUGmSY9fC1gc@ep-long-snowflake-366579.us-east-2.aws.neon.tech/neondb";
  const tables:any[] = await fetchTables(token,baseID)
  await createPostgresTable(pgUrl,tables,  tName);
  await insertRecordsToPostgres(token, baseID, tName, pgUrl);
  

  const webhookSpecifications: WebhookSpecification[] = [
    {
      options: {
        filters: {
          dataTypes: ["tableData"],
          
        },
      },
    },
    {
      options: {
        filters: {
          dataTypes: ["tableFields"],
          
        },
      },
    },
    {
      options: {
        filters: {
          dataTypes: ["tableMetadata"],
          
        },
      },
    },
  ];
  const webhooks = await manageWebhooks(
    webhookSpecifications,
    "https://6f8e-213-230-112-222.ngrok-free.app/notif-ping",
    token,
    baseID
  );``

   for (const webhook of webhooks) {
    const webhookPayloads = await fetchWebhookPayload(baseID, token, webhook.id);

    
    for (const webhookPayload of webhookPayloads) {
      await updatePostgresTable(pgUrl, webhookPayload,token,baseID);
    }
  }
}

main();
