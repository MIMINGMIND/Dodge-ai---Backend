const fs = require('fs');
const path = require('path');

const DATA_DIR = path.join(__dirname, 'data');
if (!fs.existsSync(DATA_DIR)) {
  fs.mkdirSync(DATA_DIR, { recursive: true });
}

// Helpers
const randomInt = (min, max) => Math.floor(Math.random() * (max - min + 1)) + min;
const randomChoice = (arr) => arr[randomInt(0, arr.length - 1)];
const generateId = (prefix, num, padding) => `${prefix}${String(num).padStart(padding, '0')}`;

// Master Data
const products = [
  { ProductID: 'P001', Name: 'Laptop Pro 15', Category: 'Electronics', Price: 1200 },
  { ProductID: 'P002', Name: 'Office Chair', Category: 'Furniture', Price: 250 },
  { ProductID: 'P003', Name: 'Mechanical Keyboard', Category: 'Electronics', Price: 100 },
  { ProductID: 'P004', Name: 'Wireless Mouse', Category: 'Electronics', Price: 50 },
  { ProductID: 'P005', Name: 'Standing Desk', Category: 'Furniture', Price: 600 }
];

const customers = [
  { CustomerID: 'C001', Name: 'Acme Corp', Region: 'NA' },
  { CustomerID: 'C002', Name: 'Global Tech', Region: 'EMEA' },
  { CustomerID: 'C003', Name: 'Startup Inc', Region: 'NA' },
  { CustomerID: 'C004', Name: 'Retail World', Region: 'APAC' }
];

// Transaction Data Generation
const NUM_ORDERS = 50;

const sales_order_headers = [];
const sales_order_items = [];
const outbound_delivery_headers = [];
const outbound_delivery_items = [];
const billing_document_headers = [];
const billing_document_items = [];
const journal_entries = [];

let orderCounter = 1000;
let deliveryCounter = 2000;
let billingCounter = 3000;
let journalCounter = 4000;

for (let i = 0; i < NUM_ORDERS; i++) {
  const soId = generateId('SO', orderCounter++, 4);
  const customer = randomChoice(customers);
  const orderDate = `2025-05-${String(randomInt(1, 28)).padStart(2, '0')}`;
  
  sales_order_headers.push({
    SalesOrderID: soId,
    CustomerID: customer.CustomerID,
    OrderDate: orderDate,
    Status: 'Processed'
  });

  const numItems = randomInt(1, 3);
  let orderTotal = 0;

  for (let j = 1; j <= numItems; j++) {
    const product = randomChoice(products);
    const qty = randomInt(1, 5);
    const itemNet = product.Price * qty;
    orderTotal += itemNet;

    const soItemId = `${soId}-${j}`;
    sales_order_items.push({
      SalesOrderID: soId,
      SalesOrderItemID: soItemId,
      ProductID: product.ProductID,
      Quantity: qty,
      NetValue: itemNet,
      Currency: 'USD'
    });
  }

  // 10% chance order is stuck (no delivery)
  if (Math.random() < 0.1) {
    sales_order_headers[i].Status = 'Pending Delivery';
    continue; 
  }

  // Generate Delivery
  const delId = generateId('DEL', deliveryCounter++, 4);
  outbound_delivery_headers.push({
    DeliveryID: delId,
    SalesOrderID: soId,
    DeliveryDate: orderDate
  });

  const currentOrderItems = sales_order_items.filter(item => item.SalesOrderID === soId);
  currentOrderItems.forEach((item, idx) => {
    outbound_delivery_items.push({
      DeliveryID: delId,
      DeliveryItemID: `${delId}-${idx + 1}`,
      SalesOrderItemID: item.SalesOrderItemID,
      Quantity: item.Quantity
    });
  });

  // 10% chance delivered but no billing
  if (Math.random() < 0.1) {
    sales_order_headers[i].Status = 'Delivered (Not Billed)';
    continue;
  }

  // Generate Billing
  const billId = generateId('BILL', billingCounter++, 4);
  billing_document_headers.push({
    BillingDocID: billId,
    SalesOrderID: soId, // Direct ref or via delivery
    BillingDate: orderDate,
    TotalAmount: orderTotal
  });

  currentOrderItems.forEach((item, idx) => {
    billing_document_items.push({
      BillingDocID: billId,
      BillingItemID: `${billId}-${idx + 1}`,
      SalesOrderItemID: item.SalesOrderItemID,
      NetValue: item.NetValue
    });
  });

  // 10% chance billed but no journal entry yet
  if (Math.random() < 0.1) {
    sales_order_headers[i].Status = 'Billed (No JE)';
    continue;
  }

  // Generate Journal Entry
  const jeId = generateId('JE', journalCounter++, 4);
  journal_entries.push({
    JournalEntryID: jeId,
    RefBillingDocID: billId,
    Amount: orderTotal,
    Account: 'AR-Trade',
    PostingDate: orderDate
  });
}

const writeJsonl = (filename, data) => {
  const filepath = path.join(DATA_DIR, filename);
  const content = data.map(obj => JSON.stringify(obj)).join('\n');
  fs.writeFileSync(filepath, content, 'utf8');
};

writeJsonl('products.jsonl', products);
writeJsonl('customers.jsonl', customers);
writeJsonl('sales_order_headers.jsonl', sales_order_headers);
writeJsonl('sales_order_items.jsonl', sales_order_items);
writeJsonl('outbound_delivery_headers.jsonl', outbound_delivery_headers);
writeJsonl('outbound_delivery_items.jsonl', outbound_delivery_items);
writeJsonl('billing_document_headers.jsonl', billing_document_headers);
writeJsonl('billing_document_items.jsonl', billing_document_items);
writeJsonl('journal_entries.jsonl', journal_entries);

console.log('Data generation complete.');
