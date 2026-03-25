const sqlite3 = require('sqlite3').verbose();
const fs = require('fs');
const path = require('path');
const readline = require('readline');

const DB_PATH = path.join(__dirname, 'o2c.db');
const DATA_DIR = path.join(__dirname, 'data');

if (fs.existsSync(DB_PATH)) {
  fs.unlinkSync(DB_PATH);
}

const db = new sqlite3.Database(DB_PATH);

// Flatten nested objects (e.g. creationTime: {hours,minutes,seconds} → creationTime_hours, etc.)
function flattenRecord(record) {
  const flat = {};
  for (const [key, value] of Object.entries(record)) {
    if (value !== null && typeof value === 'object' && !Array.isArray(value)) {
      for (const [subKey, subVal] of Object.entries(value)) {
        flat[`${key}_${subKey}`] = subVal;
      }
    } else {
      flat[key] = value;
    }
  }
  return flat;
}

// Explicit schemas for all SAP entity tables
const SCHEMAS = {
  business_partners: `CREATE TABLE business_partners (
    businessPartner TEXT PRIMARY KEY,
    customer TEXT,
    businessPartnerCategory TEXT,
    businessPartnerFullName TEXT,
    businessPartnerGrouping TEXT,
    businessPartnerName TEXT,
    correspondenceLanguage TEXT,
    createdByUser TEXT,
    creationDate TEXT,
    firstName TEXT,
    lastName TEXT,
    organizationBpName1 TEXT,
    organizationBpName2 TEXT,
    businessPartnerIsBlocked INTEGER,
    isMarkedForArchiving INTEGER
  )`,

  business_partner_addresses: `CREATE TABLE business_partner_addresses (
    businessPartner TEXT,
    addressId TEXT,
    streetName TEXT,
    cityName TEXT,
    country TEXT,
    postalCode TEXT,
    region TEXT,
    phoneNumber TEXT,
    emailAddress TEXT,
    PRIMARY KEY (businessPartner, addressId)
  )`,

  products: `CREATE TABLE products (
    product TEXT PRIMARY KEY,
    productType TEXT,
    crossPlantStatus TEXT,
    creationDate TEXT,
    createdByUser TEXT,
    lastChangeDate TEXT,
    isMarkedForDeletion INTEGER,
    productOldId TEXT,
    grossWeight TEXT,
    weightUnit TEXT,
    netWeight TEXT,
    productGroup TEXT,
    baseUnit TEXT,
    division TEXT,
    industrySector TEXT
  )`,

  product_descriptions: `CREATE TABLE product_descriptions (
    product TEXT,
    language TEXT,
    productDescription TEXT,
    PRIMARY KEY (product, language)
  )`,

  product_plants: `CREATE TABLE product_plants (
    product TEXT,
    plant TEXT,
    PRIMARY KEY (product, plant)
  )`,

  product_storage_locations: `CREATE TABLE product_storage_locations (
    product TEXT,
    plant TEXT,
    storageLocation TEXT,
    PRIMARY KEY (product, plant, storageLocation)
  )`,

  plants: `CREATE TABLE plants (
    plant TEXT PRIMARY KEY,
    plantName TEXT,
    country TEXT,
    region TEXT,
    cityName TEXT,
    companyCode TEXT
  )`,

  sales_order_headers: `CREATE TABLE sales_order_headers (
    salesOrder TEXT PRIMARY KEY,
    salesOrderType TEXT,
    salesOrganization TEXT,
    distributionChannel TEXT,
    organizationDivision TEXT,
    soldToParty TEXT,
    creationDate TEXT,
    createdByUser TEXT,
    lastChangeDateTime TEXT,
    totalNetAmount TEXT,
    overallDeliveryStatus TEXT,
    overallOrdReltdBillgStatus TEXT,
    transactionCurrency TEXT,
    pricingDate TEXT,
    requestedDeliveryDate TEXT,
    customerPaymentTerms TEXT,
    incotermsClassification TEXT,
    incotermsLocation1 TEXT,
    headerBillingBlockReason TEXT,
    deliveryBlockReason TEXT
  )`,

  sales_order_items: `CREATE TABLE sales_order_items (
    salesOrder TEXT,
    salesOrderItem TEXT,
    salesOrderItemCategory TEXT,
    material TEXT,
    requestedQuantity TEXT,
    requestedQuantityUnit TEXT,
    transactionCurrency TEXT,
    netAmount TEXT,
    materialGroup TEXT,
    productionPlant TEXT,
    storageLocation TEXT,
    salesDocumentRjcnReason TEXT,
    itemBillingBlockReason TEXT,
    PRIMARY KEY (salesOrder, salesOrderItem)
  )`,

  sales_order_schedule_lines: `CREATE TABLE sales_order_schedule_lines (
    salesOrder TEXT,
    salesOrderItem TEXT,
    scheduleLine TEXT,
    requestedDeliveryDate TEXT,
    confirmedDeliveryDate TEXT,
    scheduledQuantity TEXT,
    PRIMARY KEY (salesOrder, salesOrderItem, scheduleLine)
  )`,

  outbound_delivery_headers: `CREATE TABLE outbound_delivery_headers (
    deliveryDocument TEXT PRIMARY KEY,
    creationDate TEXT,
    shippingPoint TEXT,
    overallGoodsMovementStatus TEXT,
    overallPickingStatus TEXT,
    deliveryBlockReason TEXT,
    headerBillingBlockReason TEXT,
    actualGoodsMovementDate TEXT
  )`,

  outbound_delivery_items: `CREATE TABLE outbound_delivery_items (
    deliveryDocument TEXT,
    deliveryDocumentItem TEXT,
    actualDeliveryQuantity TEXT,
    deliveryQuantityUnit TEXT,
    referenceSdDocument TEXT,
    referenceSdDocumentItem TEXT,
    plant TEXT,
    storageLocation TEXT,
    batch TEXT,
    itemBillingBlockReason TEXT,
    lastChangeDate TEXT,
    PRIMARY KEY (deliveryDocument, deliveryDocumentItem)
  )`,

  billing_document_headers: `CREATE TABLE billing_document_headers (
    billingDocument TEXT PRIMARY KEY,
    billingDocumentType TEXT,
    creationDate TEXT,
    billingDocumentDate TEXT,
    billingDocumentIsCancelled INTEGER,
    cancelledBillingDocument TEXT,
    totalNetAmount TEXT,
    transactionCurrency TEXT,
    companyCode TEXT,
    fiscalYear TEXT,
    accountingDocument TEXT,
    soldToParty TEXT
  )`,

  billing_document_items: `CREATE TABLE billing_document_items (
    billingDocument TEXT,
    billingDocumentItem TEXT,
    material TEXT,
    billingQuantity TEXT,
    billingQuantityUnit TEXT,
    netAmount TEXT,
    transactionCurrency TEXT,
    referenceSdDocument TEXT,
    referenceSdDocumentItem TEXT,
    PRIMARY KEY (billingDocument, billingDocumentItem)
  )`,

  billing_document_cancellations: `CREATE TABLE billing_document_cancellations (
    billingDocument TEXT PRIMARY KEY,
    billingDocumentType TEXT,
    creationDate TEXT,
    billingDocumentDate TEXT,
    billingDocumentIsCancelled INTEGER,
    totalNetAmount TEXT,
    transactionCurrency TEXT,
    companyCode TEXT,
    fiscalYear TEXT,
    accountingDocument TEXT,
    soldToParty TEXT
  )`,

  journal_entry_items_accounts_receivable: `CREATE TABLE journal_entry_items_accounts_receivable (
    companyCode TEXT,
    fiscalYear TEXT,
    accountingDocument TEXT,
    accountingDocumentItem TEXT,
    glAccount TEXT,
    referenceDocument TEXT,
    customer TEXT,
    costCenter TEXT,
    profitCenter TEXT,
    transactionCurrency TEXT,
    amountInTransactionCurrency TEXT,
    companyCodeCurrency TEXT,
    amountInCompanyCodeCurrency TEXT,
    postingDate TEXT,
    documentDate TEXT,
    accountingDocumentType TEXT,
    assignmentReference TEXT,
    lastChangeDateTime TEXT,
    financialAccountType TEXT,
    clearingDate TEXT,
    clearingAccountingDocument TEXT,
    clearingDocFiscalYear TEXT,
    PRIMARY KEY (companyCode, fiscalYear, accountingDocument, accountingDocumentItem)
  )`,

  payments_accounts_receivable: `CREATE TABLE payments_accounts_receivable (
    companyCode TEXT,
    fiscalYear TEXT,
    accountingDocument TEXT,
    accountingDocumentItem TEXT,
    clearingDate TEXT,
    clearingAccountingDocument TEXT,
    clearingDocFiscalYear TEXT,
    amountInTransactionCurrency TEXT,
    transactionCurrency TEXT,
    amountInCompanyCodeCurrency TEXT,
    companyCodeCurrency TEXT,
    customer TEXT,
    invoiceReference TEXT,
    invoiceReferenceFiscalYear TEXT,
    salesDocument TEXT,
    salesDocumentItem TEXT,
    postingDate TEXT,
    documentDate TEXT,
    assignmentReference TEXT,
    glAccount TEXT,
    financialAccountType TEXT,
    profitCenter TEXT,
    costCenter TEXT,
    PRIMARY KEY (companyCode, fiscalYear, accountingDocument, accountingDocumentItem)
  )`,

  customer_company_assignments: `CREATE TABLE customer_company_assignments (
    customer TEXT,
    companyCode TEXT,
    paymentTerms TEXT,
    PRIMARY KEY (customer, companyCode)
  )`,

  customer_sales_area_assignments: `CREATE TABLE customer_sales_area_assignments (
    customer TEXT,
    salesOrganization TEXT,
    distributionChannel TEXT,
    division TEXT,
    PRIMARY KEY (customer, salesOrganization, distributionChannel, division)
  )`,
};

// Get schema columns (field names only) for a given table
function getSchemaColumns(tableName) {
  const schema = SCHEMAS[tableName];
  if (!schema) return null;
  const lines = schema.split('\n').slice(1); // skip CREATE TABLE line
  return lines
    .map(l => l.trim())
    .filter(l => l && !l.startsWith('PRIMARY KEY') && !l.startsWith(')'))
    .map(l => l.split(' ')[0].replace(/,$/, ''));
}

async function loadDirectory(tableName, dirPath) {
  if (!fs.existsSync(dirPath)) {
    console.log(`Skipping ${tableName}: directory not found`);
    return;
  }

  const files = fs.readdirSync(dirPath).filter(f => f.endsWith('.jsonl'));
  if (files.length === 0) {
    console.log(`Skipping ${tableName}: no JSONL files`);
    return;
  }

  const knownColumns = getSchemaColumns(tableName);

  for (const file of files) {
    const filepath = path.join(dirPath, file);
    await new Promise((resolve) => {
      const fileStream = fs.createReadStream(filepath);
      const rl = readline.createInterface({ input: fileStream, crlfDelay: Infinity });

      let stmt = null;
      let insertColumns = null;

      db.serialize(() => {
        rl.on('line', (line) => {
          if (!line.trim()) return;
          let record;
          try {
            record = JSON.parse(line);
          } catch (e) {
            return;
          }

          const flat = flattenRecord(record);

          if (!stmt) {
            // Use only columns that exist in schema
            insertColumns = knownColumns
              ? knownColumns.filter(col => flat[col] !== undefined || flat[col] === null)
              : Object.keys(flat);

            if (knownColumns) {
              // Use all schema columns, filling missing with null
              insertColumns = knownColumns;
            }

            const placeholders = insertColumns.map(() => '?').join(',');
            stmt = db.prepare(
              `INSERT OR IGNORE INTO ${tableName} (${insertColumns.join(',')}) VALUES (${placeholders})`
            );
          }

          const values = insertColumns.map(col => {
            const v = flat[col];
            if (v === undefined) return null;
            if (typeof v === 'boolean') return v ? 1 : 0;
            return v;
          });

          stmt.run(values);
        });

        rl.on('close', () => {
          if (stmt) stmt.finalize();
          console.log(`  Loaded ${file}`);
          resolve();
        });
      });
    });
  }
  console.log(`✓ ${tableName}`);
}

async function main() {
  // Create all tables
  await new Promise((resolve) => {
    db.serialize(() => {
      Object.values(SCHEMAS).forEach(schema => db.run(schema));
      resolve();
    });
  });

  // Load each entity from its subdirectory
  for (const tableName of Object.keys(SCHEMAS)) {
    const dirPath = path.join(DATA_DIR, tableName);
    await loadDirectory(tableName, dirPath);
  }

  console.log('\nDatabase loading complete.');
  db.close();
}

main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
