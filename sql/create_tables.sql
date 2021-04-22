create table if not exists orders (
    id                      bigint not null,
    closed_at               timestamp with time zone,
    created_at              timestamp with time zone,
    updated_at              timestamp with time zone,
    number                  bigint,
    note                    text,
    token                   text,
    gateway                 varchar(20),
    test                    boolean,
    total_price             money,
    subtotal_price          money,
    total_weight            int,
    total_tax               money,
    taxes_included          boolean,
    currency                char(3),
    financial_status        varchar(20),
    confirmed               boolean,
    total_discounts         money,
    total_line_items_price  money,
    cart_token              varchar(20),
    buyer_accepts_marketing boolean,
    name                    text,
    referring_site          text,
    landing_site            text,
    cancelled_at            timestamp with time zone,
    cancel_reason           text,
    total_price_usd         money,
    checkout_token          text,
    reference               text,
    user_id                 bigint,
    location_id             bigint,
    source_identifier       varchar(20),
    source_url              text,
    processed_at            timestamp with time zone,
    device_id               bigint,
    app_id                  bigint,
    browser_ip              varchar(32),
    landing_site_ref        text,
    order_number            bigint,
    processing_method       varchar(20),
    checkout_id             bigint,
    source_name             varchar(20),
    fulfillment_status      varchar(20),
    tags                    varchar(20),
    contact_email           text,
    order_status_url        text,

    primary key (id)
);

create table if not exists users (
    id              bigint not null,
    email           text,
    phone           char(12),
    customer_locale text,

    primary key (id)
);

create table if not exists order_line_items (
    id          bigint not null,
    order_id    bigint,
    variant_id  bigint,
    product_id  bigint,
    quantity    int,

    primary key (id)
);
