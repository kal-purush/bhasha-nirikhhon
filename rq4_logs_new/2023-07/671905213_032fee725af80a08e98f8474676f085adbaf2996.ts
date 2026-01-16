export interface BetterUptimeResponse {
  data: Server[];
  pagination: Pagination;
}

interface Root {
  data: Server[];
  pagination: Pagination;
}

export interface Server {
  id: string;
  type: string;
  attributes: Attributes;
  relationships: Relationships;
}

interface Attributes {
  url: string;
  pronounceable_name: string;
  auth_username: string;
  auth_password: string;
  monitor_type: string;
  monitor_group_id: string | null;
  last_checked_at: string | null;
  status: string;
  policy_id: string | null;
  required_keyword: string | null;
  verify_ssl: boolean;
  check_frequency: number;
  call: boolean;
  sms: boolean;
  email: boolean;
  push: boolean;
  team_wait: null;
  http_method: string;
  request_timeout: number;
  recovery_period: number;
  request_headers: RequestHeader[] | null;
  request_body: string;
  follow_redirects: boolean;
  remember_cookies: boolean;
  created_at: string;
  updated_at: string;
  ssl_expiration: number;
  domain_expiration: number;
  regions: string[];
  expected_status_codes: number[] | null;
  port: number | null;
  confirmation_period: number;
  paused_at: string | null;
  paused: boolean;
  maintenance_from: string | null;
  maintenance_to: string | null;
  maintenance_timezone: string;
}

interface Relationships {
  policy: Policy;
}

interface Policy {
  data: any;
}

export interface Pagination {
  first: string;
  last: string;
  prev: string | null;
  next: string | null;
}

interface RequestHeader {
  id: string;
  name: string;
  value: string;
}