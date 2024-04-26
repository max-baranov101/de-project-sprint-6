INSERT INTO
	STV202404101__DWH.l_user_group_activity(
		hk_l_user_group_activity,
		hk_user_id,
		hk_group_id,
		load_dt,
		load_src
	)
select
	distinct hash(hk_user_id, hk_group_id) as hk_l_user_group_activity,
	hk_user_id,
	hk_group_id,
	now() as load_dt,
	's3' as load_src
from
	STV202404101__STAGING.group_log as sgl
	left join STV202404101__DWH.h_users hu on hu.user_id = sgl.user_id
	left join STV202404101__DWH.h_groups hg on hg.group_id = sgl.group_id
where
	hash(hk_user_id, hk_group_id) not in (
		select
			hk_l_user_group_activity
		from
			STV202404101__DWH.l_user_group_activity
	);