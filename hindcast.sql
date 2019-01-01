create table grid_element (
    element_id integer not null,
    primary key (element_id)
);

select AddGeometryColumn('public', 'grid_element', 'geom', 4326, 'POLYGON', 2);

create table grid_node (
    node_id integer not null,
    primary key (node_id),
    depth double precision not null
);

select AddGeometryColumn('public', 'grid_node', 'geom', 4326, 'POINT', 2);

