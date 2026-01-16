import { Injectable } from '@nestjs/common';
import { TracingLoggerService } from '../../../logger/tracing-logger.service';
import { AxiosInstance } from 'axios';
import { ConfigService } from '@nestjs/config';
import axios from 'axios';
import * as https from 'https';
import { commonBody } from '../sync.constant';

@Injectable()
export class SyncDataService {
  private readonly axios: AxiosInstance;
  private readonly username: string;
  private readonly password: string;

  constructor(
    private readonly logger: TracingLoggerService,
    private readonly configService: ConfigService,
  ) {
    this.logger.setContext(SyncDataService.name);
    this.axios = axios.create({
      baseURL: this.configService.get<string>('BASE_URL'),
      httpsAgent: new https.Agent({ rejectUnauthorized: false }),
    });
    this.username = this.configService.get<string>('USERNAME');
    this.password = this.configService.get<string>('PASSWORD');
  }

  async getAuthorize() {
    this.logger.debug('[SYNC DATA] - Login to web');
    await this.axios.get(`/default.aspx`);
    const payload = {
      ...commonBody,
      ctl00$ContentPlaceHolder1$ctl00$ucDangNhap$btnDangNhap: 'Đăng Nhập',
      ctl00$ContentPlaceHolder1$ctl00$ucDangNhap$txtTaiKhoa: this.username,
      ctl00$ContentPlaceHolder1$ctl00$ucDangNhap$txtMatKhau: this.password,
    };


    const result = await this.axios.post('/Default.aspx?page=gioithieu', payload, {
      withCredentials: true,
    });

    // Extract the 'set-cookie' header
    const setCookieHeader = result.headers['set-cookie'];
    if (!setCookieHeader) {
      throw new Error('No Set-Cookie header received from the response');
    }
    this.logger.debug('[SYNC DATA] - Parsed Cookies');
    return this.parseCookies(setCookieHeader);
  }

  // Helper function to parse 'set-cookie' header into a key-value object
  parseCookies(setCookieHeader: string[]): string {
    return setCookieHeader.map(cookie => cookie.split(';')[0]).join('; ');
  }

  async syncData() {
    const cookie = await this.getAuthorize();
    console.log(cookie);
    this.logger.debug('[SYNC DATA] - Sync data from web');
    const headers = {
      Cookie: cookie,
    };

    const result = await this.axios.get('/Default.aspx?page=ctdtkhoisv',{ headers } );

    const data = result.data;
    if (data) {
      this.logger.debug('[SYNC DATA] - Sync data from web successfully');
    }
    return data;
  }
}