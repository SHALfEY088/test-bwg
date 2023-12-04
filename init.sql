create table if not exists public.withdraw
(
    id         serial
    primary key,
    amount     numeric                                not null,
    created_at timestamp with time zone default now() not null,
    client_id  integer                                not null
    );

create table if not exists public.invoice
(
    id         serial
    primary key,
    amount     numeric                                not null,
    created_at timestamp with time zone default now() not null,
    client_id  integer                                not null
    );