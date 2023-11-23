import { Injectable } from '@nestjs/common';
import { Database } from "duckdb";

const db = new Database(':memory:');

@Injectable()
export class AppService {
   getHello(): string {

      db.all(`
         INSTALL spatial; LOAD spatial;
         CREATE OR REPLACE TABLE CAT_CONVERSION AS SELECT * FROM st_read('./src/CAT_CONVERSION.xlsx');
         COPY CAT_CONVERSION TO 'CAT_CONVERSION.parquet'(FORMAT PARQUET);
      `, function (err, res) {
         if (err) console.log(err)
         console.log(res)
      });

      db.all(`CREATE OR REPLACE TABLE CAT_BOM_MX AS SELECT * FROM read_csv_auto("./src/CAT_BOM_MX_09AGO23.csv");
      COPY CAT_BOM_MX TO 'CAT_BOM_MX.parquet'(FORMAT PARQUET);`, function (err, res) {
         if (err) console.log(err)
         console.log(res)
      });

      db.all(`CREATE OR REPLACE TABLE CAT_BOM_NA AS SELECT * FROM read_csv_auto('./src/CAT_BOM_NA_09AGO23.csv');
      COPY CAT_BOM_NA TO 'CAT_BOM_NA.parquet'(FORMAT PARQUET);`, function (err, res) {
         if (err) console.log(err)
         console.log(res)
      });

      db.all(`CREATE OR REPLACE TABLE CAT_SALES_VOLUME AS SELECT * FROM read_csv_auto('./src/CAT_SALES_VOLUME_09AGO23.csv', ignore_errors=1);
      COPY CAT_SALES_VOLUME TO 'CAT_SALES_VOLUME.parquet'(FORMAT PARQUET);`, function (err, res) {
         if (err) console.log(err)
         console.log(res)
      });

      db.all(`CREATE OR REPLACE TABLE FAC_PO AS SELECT * FROM st_read('./src/Base PO 09AGO23.xlsx');
      COPY FAC_PO TO 'FAC_PO.parquet'(FORMAT PARQUET);`, function (err, res) {
         if (err) console.log(err)
         console.log(res)
      });
      
      db.all(`CREATE OR REPLACE TABLE CAT_CUSTOMER_FG AS SELECT * FROM st_read('./src/CAT_CUSTOMER_FG.xlsx');
      COPY CAT_CUSTOMER_FG TO 'CAT_CUSTOMER_FG.parquet'(FORMAT PARQUET);`, function (err, res) {
         if (err) console.log(err)
         console.log(res)
      });

      db.all(`CREATE OR REPLACE TABLE CAT_PLANT AS SELECT * FROM st_read('./src/CAT_PLANT.xlsx');
      COPY CAT_PLANT TO 'CAT_PLANT.parquet'(FORMAT PARQUET);`, function (err, res) {
         if (err) console.log(err)
         console.log(res)
      });

      db.all(`CREATE OR REPLACE TABLE CAT_STANDARD_PRICE AS SELECT * FROM st_read('./src/CAT_STANDARD_PRICE_13JUN23.xlsx');
      COPY CAT_STANDARD_PRICE TO 'CAT_STANDARD_PRICE.parquet'(FORMAT PARQUET);`, function (err, res) {
         if (err) console.log(err)
         console.log(res)
      });

      return 'Todo ok';
   }
}


