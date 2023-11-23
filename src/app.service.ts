import { Injectable } from '@nestjs/common';
import { Database } from "duckdb";

const db = new Database(':memory:');

@Injectable()
export class AppService {
   getHello(): string {

 
      return 'Todo ok';
   }
}


