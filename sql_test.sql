create multiset table data_lab_mktg_tbls.nr_di_v_adobe_dm as (
    select cnst_mstr_id,
    src_cd, 
    intrctn_dt, 
    min(delivery_key) delivery_key,
    sum(mail_drop_cnt) mail_cnt, 
	cast('20'||substr(src_cd,4,2)||'-'||substr(src_cd,6,2)||'-01' as date) as ym,
	row_number() over (partition by cnst_mstr_id, ym order by intrctn_dt, src_cd) rn
from mktg_ops_vws.bzfc_fact_dmail_interaction
where mail_drop_cnt>0
    and substr(src_cd,1,3) in ('RQA','RQC','RQS') 
    and src_cd not in ('RQA19100M003') -- Oct 2019 Disaster Postcard (No Financial Ask)
    and intrctn_dt between add_months(date,-48) and trunc(date,'MM')-1
group by 1,2,3
)with data primary index (cnst_mstr_id, src_cd);
