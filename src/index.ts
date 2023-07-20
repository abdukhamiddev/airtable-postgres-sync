import axios, { AxiosResponse } from "axios";
import pg, { Pool } from "pg";

const baseID = "appff8HJFMBuoXgwd";
const tName = "Vendors";
const token =
  "patwI1yOnv2nlWULt.2d3d51fcb8653f64bac88467c64e063ae1eafbfdfd67823f60a9a0d3be13183c";

const pgUrl =
  "postgres://latipovabdukhamid:dUGmSY9fC1gc@ep-long-snowflake-366579.us-east-2.aws.neon.tech/neondb";

async function fetchTables(): Promise<any[]> {
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

async function fetchTableRecords(tableName: string) {
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

async function createPostgresTable() {
    const tables: any[] = await fetchTables();
    const table = tables.find((t: any) => t.name === tName);
    const pool = new Pool({ connectionString: pgUrl, ssl: true });
  
    if (!table) {
      console.log("Error fetching tables");
      return;
    }
  
    const client = await pool.connect();
    console.log("Table created successfully");
  
    try {
      const fields = table?.fields;
      if (!fields || fields.length === 0) {
        console.log("No fields found in the table.");
        return;
      }
  
      const columnDefinitions = fields.map((field: any) => {
        const columnName = `"${field.name}"`; 
        return `${columnName} ${mapAirtableTypeToPostgresType(field.type)}`;
      });
  
      const createTableQuery = `CREATE TABLE IF NOT EXISTS "${tName}" (${columnDefinitions.join(", ")})`;
  
      console.log("Create table query:", createTableQuery); 
  
      await client.query(createTableQuery);
      console.log("Table Created Successfully");
    } catch (error) {
      console.error("Error creating PostgreSQL table:", error);
    } finally {
      client.release();
    }
  }
  
  async function insertRecordsToPostgres() {
    const tableRecords = await fetchTableRecords(tName);
    const pool = new Pool({ connectionString: pgUrl, ssl: true });
    const client = await pool.connect();
  
    try {
      if (tableRecords.length === 0) {
        console.log("No records to insert.");
        return;
      }
  
      for (const record of tableRecords) {
        const fieldNames = Object.keys(record.fields);
        const fieldValues = Object.values(record.fields);
        const insertQuery = `
          INSERT INTO ${tName} (${fieldNames.join(', ')})
          VALUES (${fieldValues.map((value) => (typeof value === 'string' ? `'${value}'` : value)).join(', ')})
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
  await createPostgresTable();
  await insertRecordsToPostgres();
}

main();
