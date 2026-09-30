import "dotenv/config";


export const PBX_URL = process.env.THREECX_URL?.replace(/\/+$/, "");
export const CLIENT_ID = process.env.THREECX_CLIENT_ID;
export const CLIENT_SECRET = process.env.THREECX_CLIENT_SECRET;
export const DN = process.env.THREECX_DN || process.env.THREECX_ROUTE_POINT_DN;

export const DEPARTMENTS = {
  booking: process.env.BOOKING_DN,
  sales: process.env.SALES_DN,
  reception: process.env.RECEPTION_DN,
  restaurant: process.env.RESTAURANT_DN,
  spa: process.env.SPA_DN,
  it: process.env.IT_DN
}; 
export const OPENAI_API_KEY = process.env.OPENAI_API_KEY;

export const OPENAI_REALTIME_MODEL =  process.env.OPENAI_REALTIME_MODEL ||
  "gpt-realtime";

export const OPENAI_VOICE = process.env.OPENAI_VOICE;

export const OPENAI_INSTRUCTIONS = process.env.OPENAI_BOT_INSTRUCTIONS ;
