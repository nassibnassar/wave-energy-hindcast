begin;

alter table geography_columns owner to hindcast_admin;
alter table geometry_columns owner to hindcast_admin;
alter table raster_columns owner to hindcast_admin;
alter table raster_overviews owner to hindcast_admin;
alter table spatial_ref_sys owner to hindcast_admin;

commit;

