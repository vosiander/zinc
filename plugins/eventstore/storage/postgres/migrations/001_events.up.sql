create table events
(
    id text not null,
    aggregate_id text not null,
    name text not null,
    created_at timestamp,
    version text not null,
    data jsonb not null
);

create index events_asc
    on events (aggregate_id, created_at);

create unique index events_id
    on events (id);

alter table events
    add constraint events_pk
        primary key (id);