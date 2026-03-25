const express = require('express');
const cors = require('cors');
const sqlite3 = require('sqlite3').verbose();
const path = require('path');
const Groq = require('groq-sdk');

const app = express();

if (process.env.NODE_ENV !== 'production') {
  require('dotenv').config();
}

// CORS configuration
const corsOptions = {
  origin: (origin, callback) => {
    const allowedOrigins = [
      'http://localhost:5173',
      "https://dodge-ai-frontend.vercel.app"
    ];
    
    if (!origin || allowedOrigins.includes(origin)) {
      callback(null, true);
    } else {
      callback(new Error('Not allowed by CORS'));
    }
  },
  credentials: true
};

app.use(cors(corsOptions));
app.use(express.json());

const DB_PATH = path.join(__dirname, 'o2c.db');
const db = new sqlite3.Database(DB_PATH, sqlite3.OPEN_READONLY);

let ai = null;
if (process.env.GROQ_API_KEY) {
  ai = new Groq({ apiKey: process.env.GROQ_API_KEY });
}

const runQuery = (query, params = []) => {
  return new Promise((resolve, reject) => {
    db.all(query, params, (err, rows) => {
      if (err) reject(err);
      else resolve(rows);
    });
  });
};

// Limit nodes per type to keep the graph responsive
const NODE_LIMIT = 150;

app.get('/api/graph', async (req, res) => {
  try {
    const nodes = [];
    const edges = [];
    const nodeSet = new Set();

    const addNode = (node) => {
      if (!nodeSet.has(node.id)) {
        nodeSet.add(node.id);
        nodes.push(node);
      }
    };

    // --- Business Partners (Customers) ---
    const partners = await runQuery(
      `SELECT businessPartner, businessPartnerFullName, businessPartnerCategory, businessPartnerGrouping
       FROM business_partners LIMIT ${NODE_LIMIT}`
    );
    partners.forEach(bp => addNode({
      id: `BP_${bp.businessPartner}`,
      label: bp.businessPartnerFullName || bp.businessPartner,
      group: 'BusinessPartner',
      details: bp
    }));

    // --- Sales Orders ---
    const soHeaders = await runQuery(
      `SELECT salesOrder, soldToParty, salesOrderType, creationDate, totalNetAmount, transactionCurrency,
              overallDeliveryStatus, overallOrdReltdBillgStatus
       FROM sales_order_headers LIMIT ${NODE_LIMIT}`
    );
    soHeaders.forEach(so => {
      addNode({
        id: `SO_${so.salesOrder}`,
        label: so.salesOrder,
        group: 'SalesOrder',
        details: so
      });
      // Edge: BusinessPartner → SalesOrder
      if (so.soldToParty) {
        edges.push({ source: `BP_${so.soldToParty}`, target: `SO_${so.salesOrder}`, label: 'Places Order' });
      }
    });

    // --- Products (via sales order items) ---
    const soItems = await runQuery(
      `SELECT DISTINCT soi.material, soi.salesOrder, p.productOldId, p.productGroup, p.baseUnit
       FROM sales_order_items soi
       LEFT JOIN products p ON p.product = soi.material
       WHERE soi.material IS NOT NULL AND soi.material != ''
       LIMIT ${NODE_LIMIT}`
    );
    soItems.forEach(item => {
      addNode({
        id: `P_${item.material}`,
        label: item.productOldId || item.material,
        group: 'Product',
        details: { product: item.material, productOldId: item.productOldId, productGroup: item.productGroup, baseUnit: item.baseUnit }
      });
      // Edge: Product → SalesOrder
      if (nodeSet.has(`SO_${item.salesOrder}`)) {
        edges.push({ source: `P_${item.material}`, target: `SO_${item.salesOrder}`, label: 'Ordered' });
      }
    });

    // --- Outbound Deliveries ---
    const delHeaders = await runQuery(
      `SELECT deliveryDocument, creationDate, shippingPoint, overallGoodsMovementStatus, overallPickingStatus
       FROM outbound_delivery_headers LIMIT ${NODE_LIMIT}`
    );
    delHeaders.forEach(del => addNode({
      id: `DEL_${del.deliveryDocument}`,
      label: del.deliveryDocument,
      group: 'Delivery',
      details: del
    }));

    // Edges: SalesOrder → Delivery (via delivery items)
    const delItems = await runQuery(
      `SELECT DISTINCT deliveryDocument, referenceSdDocument
       FROM outbound_delivery_items
       WHERE referenceSdDocument IS NOT NULL AND referenceSdDocument != ''
       LIMIT ${NODE_LIMIT * 2}`
    );
    delItems.forEach(di => {
      if (nodeSet.has(`SO_${di.referenceSdDocument}`) && nodeSet.has(`DEL_${di.deliveryDocument}`)) {
        edges.push({ source: `SO_${di.referenceSdDocument}`, target: `DEL_${di.deliveryDocument}`, label: 'Delivered In' });
      }
    });

    // --- Billing Documents ---
    const billHeaders = await runQuery(
      `SELECT billingDocument, billingDocumentType, billingDocumentDate, totalNetAmount,
              transactionCurrency, soldToParty, billingDocumentIsCancelled
       FROM billing_document_headers
       WHERE billingDocumentIsCancelled = 0 OR billingDocumentIsCancelled IS NULL
       LIMIT ${NODE_LIMIT}`
    );
    billHeaders.forEach(bill => addNode({
      id: `BILL_${bill.billingDocument}`,
      label: bill.billingDocument,
      group: 'Billing',
      details: bill
    }));

    // Edges: Delivery → Billing (via billing items referencing delivery documents)
    const billItems = await runQuery(
      `SELECT DISTINCT billingDocument, referenceSdDocument
       FROM billing_document_items
       WHERE referenceSdDocument IS NOT NULL AND referenceSdDocument != ''
       LIMIT ${NODE_LIMIT * 2}`
    );
    billItems.forEach(bi => {
      if (nodeSet.has(`DEL_${bi.referenceSdDocument}`) && nodeSet.has(`BILL_${bi.billingDocument}`)) {
        edges.push({ source: `DEL_${bi.referenceSdDocument}`, target: `BILL_${bi.billingDocument}`, label: 'Billed As' });
      } else if (nodeSet.has(`SO_${bi.referenceSdDocument}`) && nodeSet.has(`BILL_${bi.billingDocument}`)) {
        // Fallback: link directly from SO if no delivery match
        edges.push({ source: `SO_${bi.referenceSdDocument}`, target: `BILL_${bi.billingDocument}`, label: 'Billed As' });
      }
    });

    // --- Journal Entries (AR) ---
    const journals = await runQuery(
      `SELECT DISTINCT accountingDocument, referenceDocument, glAccount, amountInTransactionCurrency,
              transactionCurrency, postingDate, customer, profitCenter
       FROM journal_entry_items_accounts_receivable
       WHERE referenceDocument IS NOT NULL AND referenceDocument != ''
       LIMIT ${NODE_LIMIT}`
    );
    journals.forEach(je => {
      addNode({
        id: `JE_${je.accountingDocument}`,
        label: je.accountingDocument,
        group: 'JournalEntry',
        details: je
      });
      // Edge: Billing → JournalEntry
      if (nodeSet.has(`BILL_${je.referenceDocument}`)) {
        edges.push({ source: `BILL_${je.referenceDocument}`, target: `JE_${je.accountingDocument}`, label: 'Posted As' });
      }
    });

    // --- Payments (AR) ---
    const payments = await runQuery(
      `SELECT DISTINCT accountingDocument, clearingAccountingDocument, amountInTransactionCurrency,
              transactionCurrency, clearingDate, customer
       FROM payments_accounts_receivable
       WHERE clearingAccountingDocument IS NOT NULL AND clearingAccountingDocument != ''
       LIMIT ${NODE_LIMIT}`
    );
    payments.forEach(pay => {
      addNode({
        id: `PAY_${pay.accountingDocument}`,
        label: pay.accountingDocument,
        group: 'Payment',
        details: pay
      });
      // Edge: JournalEntry → Payment (clearing)
      if (nodeSet.has(`JE_${pay.clearingAccountingDocument}`)) {
        edges.push({ source: `JE_${pay.clearingAccountingDocument}`, target: `PAY_${pay.accountingDocument}`, label: 'Cleared By' });
      }
    });

    res.json({ nodes, edges });
  } catch (error) {
    console.error('Graph Error:', error);
    res.status(500).json({ error: error.message });
  }
});

const SCHEMA_PROMPT = `
You are an AI assistant for an SAP Order to Cash (O2C) system.
You have access to a SQLite database with the following schema:

- business_partners(businessPartner, customer, businessPartnerCategory, businessPartnerFullName, businessPartnerGrouping, businessPartnerName, creationDate, businessPartnerIsBlocked, isMarkedForArchiving)
- business_partner_addresses(businessPartner, addressId, streetName, cityName, country, postalCode, region, phoneNumber, emailAddress)
- products(product, productType, crossPlantStatus, creationDate, productOldId, grossWeight, weightUnit, netWeight, productGroup, baseUnit, division, industrySector)
- product_descriptions(product, language, productDescription)
- product_plants(product, plant)
- product_storage_locations(product, plant, storageLocation)
- plants(plant, plantName, country, region, cityName, companyCode)
- sales_order_headers(salesOrder, salesOrderType, salesOrganization, distributionChannel, soldToParty, creationDate, totalNetAmount, overallDeliveryStatus, overallOrdReltdBillgStatus, transactionCurrency, pricingDate, requestedDeliveryDate, customerPaymentTerms, incotermsClassification)
- sales_order_items(salesOrder, salesOrderItem, salesOrderItemCategory, material, requestedQuantity, requestedQuantityUnit, transactionCurrency, netAmount, materialGroup, productionPlant, storageLocation)
- sales_order_schedule_lines(salesOrder, salesOrderItem, scheduleLine, requestedDeliveryDate, confirmedDeliveryDate, scheduledQuantity)
- outbound_delivery_headers(deliveryDocument, creationDate, shippingPoint, overallGoodsMovementStatus, overallPickingStatus, deliveryBlockReason, headerBillingBlockReason, actualGoodsMovementDate)
- outbound_delivery_items(deliveryDocument, deliveryDocumentItem, actualDeliveryQuantity, deliveryQuantityUnit, referenceSdDocument, referenceSdDocumentItem, plant, storageLocation)
- billing_document_headers(billingDocument, billingDocumentType, creationDate, billingDocumentDate, billingDocumentIsCancelled, cancelledBillingDocument, totalNetAmount, transactionCurrency, companyCode, fiscalYear, accountingDocument, soldToParty)
- billing_document_items(billingDocument, billingDocumentItem, material, billingQuantity, billingQuantityUnit, netAmount, transactionCurrency, referenceSdDocument, referenceSdDocumentItem)
- billing_document_cancellations(billingDocument, billingDocumentType, creationDate, billingDocumentIsCancelled, totalNetAmount, transactionCurrency, soldToParty)
- journal_entry_items_accounts_receivable(companyCode, fiscalYear, accountingDocument, accountingDocumentItem, glAccount, referenceDocument, customer, costCenter, profitCenter, transactionCurrency, amountInTransactionCurrency, companyCodeCurrency, amountInCompanyCodeCurrency, postingDate, documentDate, accountingDocumentType, clearingDate, clearingAccountingDocument)
- payments_accounts_receivable(companyCode, fiscalYear, accountingDocument, accountingDocumentItem, clearingDate, clearingAccountingDocument, amountInTransactionCurrency, transactionCurrency, customer, glAccount, postingDate, documentDate, profitCenter)
- customer_company_assignments(customer, companyCode, paymentTerms)
- customer_sales_area_assignments(customer, salesOrganization, distributionChannel, division)

Key relationships:
- sales_order_headers.soldToParty → business_partners.businessPartner
- sales_order_items.salesOrder → sales_order_headers.salesOrder
- sales_order_items.material → products.product
- outbound_delivery_items.referenceSdDocument → sales_order_headers.salesOrder (the SO fulfilled)
- outbound_delivery_items.deliveryDocument → outbound_delivery_headers.deliveryDocument
- billing_document_items.referenceSdDocument → outbound_delivery_headers.deliveryDocument (delivery being billed)
- billing_document_items.billingDocument → billing_document_headers.billingDocument
- journal_entry_items_accounts_receivable.referenceDocument → billing_document_headers.billingDocument
- payments_accounts_receivable.clearingAccountingDocument → journal_entry_items_accounts_receivable.accountingDocument

When the user asks a question:
- If the question is related to the Order to Cash data above, generate ONLY a valid SQL SELECT query to answer it. Do not include \`\`\`sql blocks, just the raw SQL.
- If the question is NOT related to Order to Cash, SAP data, or any of the tables/fields above, respond with exactly: OUT_OF_CONTEXT
`;

app.post('/api/chat', async (req, res) => {
  try {
    const { prompt } = req.body;

    if (!ai) {
      return res.status(500).json({ error: 'GROQ_API_KEY not configured.' });
    }

    const sqlResponse = await ai.chat.completions.create({
      model: 'llama-3.1-8b-instant',
      messages: [
        { role: 'system', content: SCHEMA_PROMPT },
        { role: 'user', content: 'User question: ' + prompt }
      ]
    });

    let sqlQuery = sqlResponse.choices[0]?.message?.content?.trim() || '';
    if (sqlQuery.startsWith('```sql')) {
      sqlQuery = sqlQuery.replace(/^```sql/, '').replace(/```$/, '').trim();
    } else if (sqlQuery.startsWith('```')) {
      sqlQuery = sqlQuery.replace(/^```/, '').replace(/```$/, '').trim();
    }

    console.log('AI Generated SQL:', sqlQuery);

    if (sqlQuery.toUpperCase() === 'OUT_OF_CONTEXT') {
      return res.json({ reply: "I can only answer questions about the Order to Cash process. Please ask about sales orders, deliveries, billing, payments, customers, or products." });
    }

    if (!sqlQuery.toUpperCase().startsWith('SELECT')) {
      return res.json({ reply: "I cannot answer that. Please ask a question related to the Order to Cash data." });
    }

    const results = await runQuery(sqlQuery);
    console.log('Query Results Length:', results.length);

    const nlgPrompt = `
You are a supply chain analyst AI answering a question based on SAP Order to Cash data.
User Question: "${prompt}"
SQL Executed: "${sqlQuery}"
Query Results (JSON): ${JSON.stringify(results).substring(0, 3000)}

Provide a clear, concise, and professional answer. If results include IDs (Sales Orders, Billing Documents, etc.), mention them so the user can find them in the graph visualization. Use markdown formatting for readability.
`;

    const finalResponse = await ai.chat.completions.create({
      model: 'llama-3.1-8b-instant',
      messages: [{ role: 'user', content: nlgPrompt }]
    });

    res.json({ reply: finalResponse.choices[0]?.message?.content || '', sqlQuery, data: results });

  } catch (error) {
    console.error('Chat Error:', error);
    res.status(500).json({ error: error.message, reply: 'I encountered an error analyzing your request.' });
  }
});

const PORT = 3001;
app.listen(PORT, () => {
  console.log(`Backend running on port ${PORT}`);

});
module.exports = app;
