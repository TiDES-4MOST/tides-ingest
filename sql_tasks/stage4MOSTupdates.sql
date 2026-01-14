SELECT *
from tides_master
where tides_master.tides_id in (
        select tides_id
        from tides_stage
    )
    or tides_master.tides_id in (
        select tides_id
        from to_deactivate
    );
-- TODO: join stage and master on ra, dec rather than name