src=[9,11,13,17,36,38,39]

for x in src:
    txt = f"""select count(*),ad.customer_id
 FROM adp_core.dl{x}.AGENTCREDENTIAL AC INNER JOIN adp_core.dwh.AGT_AGENT_DETAILS AD ON AC.OWNERID=AD.AGENT_ENTITY_NUM AND AD.AUDIT_TABLE_ID IN (SELECT OBJ_ID FROM adp_core.ADMIN.AUD_OBJECT_MASTER WHERE OBJ_NAME='ODS_AGENTDATA_WF' AND JOB_ID=1) and Ad.audit_sys_id=12 and ad.customer_id=ac.customer_id
 where ac.active_flag = 'YES'
 group by AD.customer_id
 union all  """
    print(txt)
