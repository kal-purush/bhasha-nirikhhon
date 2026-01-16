    export interface Core {
        core_serial: string;
        flight?: number;
        block?: number;
        reused?: boolean;
        land_success?: boolean;
        landing_type: string;
        landing_vehicle: string;
    }

    export interface FirstStage {
        cores: Core[];
    }

    export interface Payload {
        payload_id: string;
        reused?: boolean;
        customers: string[];
        payload_type: string;
        payload_mass_kg?: number;
        payload_mass_lbs?: number;
        orbit: string;
        cap_serial: string;
        mass_returned_kg?: number;
        mass_returned_lbs?: number;
        flight_time_sec?: number;
        cargo_manifest: string;
    }

    export interface SecondStage {
        payloads: Payload[];
    }

    export interface Rocket {
        rocket_id: string;
        rocket_name: string;
        rocket_type: string;
        first_stage: FirstStage;
        second_stage: SecondStage;
    }

    export interface Telemetry {
        flight_club: string;
    }

    export interface Reuse {
        core: boolean;
        side_core1: boolean;
        side_core2: boolean;
        fairings: boolean;
        capsule: boolean;
    }

    export interface LaunchSite {
        site_id: string;
        site_name: string;
        site_name_long: string;
    }

    export interface Links {
        mission_patch: string;
        mission_patch_small: string;
        article_link: string;
        wikipedia: string;
        video_link: string;
        presskit: string;
        reddit_campaign: string;
        reddit_launch: string;
        reddit_recovery: string;
        reddit_media: string;
    }

    export interface Launch {
        flight_number: number;
        mission_name: string;
        launch_year: string;
        launch_date_unix: number;
        launch_date_utc: Date;
        launch_date_local: Date;
        rocket: Rocket;
        telemetry: Telemetry;
        reuse: Reuse;
        launch_site: LaunchSite;
        launch_success?: boolean;
        links: Links;
        details: string;
    }