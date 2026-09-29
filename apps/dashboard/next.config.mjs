/** @type {import('next').NextConfig} */
const nextConfig = {
  reactStrictMode: true,
  env: {
    DURABLY_API_URL: process.env.DURABLY_API_URL ?? 'http://localhost:3000',
    DURABLY_API_KEY: process.env.DURABLY_API_KEY ?? ''
  }
};

export default nextConfig;
