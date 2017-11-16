SELECT DISTINCT
  'marin_tracker' as 'data_source',
  parsed.referring_domain,
  parsed.entry_referrer,
  parsed.entry_url,
  parsed.ojid,
  CASE WHEN parsed.action_id = 1 THEN
    'Click'
  WHEN parsed.action_id = 2 THEN
    'Conversion'
  END AS 'event_type',
  coalesce(parsed.device, parsed.device_combined) as 'event_device',
  parsed.receive_time AS 'event_time',
  replace(replace(ct.abbreviation, ' (Conv Date)', ''), ' (Click Date)', '') as 'conversion_type',
  ct.conversion_type_id,
  parsed.order_type as 'conversion_extended_data',
  parsed.uuid as 'uuid',
  parsed.order_id AS 'order_id',
  c.client_id as 'client_id',
  c.client_name AS 'client_name',
  p.publisher AS 'publisher',
  pca.alias AS 'account',
  pc.campaign_name AS 'campaign',
  pc.publisher_campaign_id As 'marin_campaign_id', 
  pc.ext_id AS 'publisher_campaign_id',
  pc.distribution_types As 'campaign_network_enum', 
  pg.publisher_group_name AS 'ad_set',
  pg.ext_id AS 'publisher_adset_id',
  parsed.mkwid as 'marin_tracking_value',
  ki.ext_id as 'publisher_keyword_id',
  ki.id as 'marin_keyword_id',
  COALESCE(ki.keyword, parsed.keyword) AS 'keyword_text',
  COALESCE(ki.keyword_type, CASE
    WHEN parsed.match_type IN ('b', 'bb') THEN
      'BROAD'
    WHEN parsed.match_type IN ('e', 'be') THEN
      'EXACT'
    WHEN parsed.match_type IN ('p', 'bp') THEN
      'PHRASE'
    END) AS 'keyword_match_type',
  IF(pcr_ext.creative_id, pcr_ext.ext_id, pcr.ext_id) as 'publisher_creative_id',
  IF(pcr_ext.creative_id, pcr_ext.creative_id, pcr.creative_id) as 'marin_creative_id',
  IF(pcr_ext.creative_id, pcr_ext.headline, pcr.headline) AS 'creative_headline',
  '' as 'creative_headline_1',
  '' as 'creative_headline_2',
  IF(pcr_ext.creative_id, pcr_ext.description1, pcr.description1) AS 'creative_description_1',
  IF(pcr_ext.creative_id, pcr_ext.description2, pcr.description2) AS 'creative_description_2',
  IF(pcr_ext.creative_id, pcr_ext.display_url, pcr.display_url) AS 'creative_display_url',
  pl.placement_url as 'placement',
  pt.product_target_name as 'product_group',
  parsed.product_name AS 'product_name',
  parsed.category AS 'category',
  SUBSTRING_INDEX(parsed.quantity, '|', 1) AS 'conversions',
  parsed.quantity as 'full_quantity_string',
  SUBSTRING_INDEX(parsed.price, '|', 1) AS 'revenue',
  parsed.price as 'full_revenue_string',
  parsed.currency AS 'currency',
  case when 'dim1' = '' then '' else coalesce(dim1_keyword.md_value, dim1_creative.md_value, dim1_group.md_value, dim1_campaign.md_value, dim1_account.md_value) end AS dim_1_value,
  case when 'dim2' = '' then '' else coalesce(dim2_keyword.md_value, dim2_creative.md_value, dim2_group.md_value, dim2_campaign.md_value, dim2_account.md_value) end As dim_2_value
FROM
(
  SELECT deduped.day,
    deduped.order_id as ojid,
    CONVERT_TZ(td.receive_time, 'GMT', deduped.conversion_time) as conversion_time,
    CONVERT_TZ(td.receive_time, 'GMT', deduped.last_click_time) as last_click_time,
    clients.client_id as parent_client_id,
    clients.customer_id,
    deduped.uuid,
    td.action_id,
    date_format(CONVERT_TZ(td.receive_time, 'GMT', 'EST'), '%Y-%m-%d %T') as receive_time,
    td.order_type,
    td.order_id,
    td.product_name,
    td.category,
    td.quantity,
    td.price,
    td.keyword_instance_id,
    td.creative_id,
    td.publisher_group_id,
    td.entry_url,
    td.referring_domain,
    td.entry_referrer,
    case when td.entry_url regexp CONCAT(mkwid.param_name, '=') then
      SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(td.entry_url, CONCAT(mkwid.param_name, '='), -1), mkwid.param_stop, 1), coalesce(device_delimiter.setting_value, '_'), 1)
    when td.entry_url regexp CONCAT(mkwid.param_name, mkwid.param_stop) then
      SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(td.entry_url, CONCAT(mkwid.param_name, mkwid.param_stop), -1), mkwid.param_stop, 1), coalesce(device_delimiter.setting_value, '_'), 1)
    end as mkwid,
    case when td.entry_url regexp CONCAT(pcrid.param_name, '=') then
      SUBSTRING_INDEX(SUBSTRING_INDEX(td.entry_url, CONCAT(pcrid.param_name, '='), -1), pcrid.param_stop, 1)
    when td.entry_url regexp CONCAT(pcrid.param_name, pcrid.param_stop) then
      SUBSTRING_INDEX(SUBSTRING_INDEX(td.entry_url, CONCAT(pcrid.param_name, pcrid.param_stop), -1), pcrid.param_stop, 1)
    end as pcrid,
    case when td.entry_url REGEXP CONCAT(pkw.param_name, '=') then
      SUBSTRING_INDEX(SUBSTRING_INDEX(marin.urldecode(td.entry_url), concat(pkw.param_name, '='), -1), pkw.param_stop, 1)
    when td.entry_url REGEXP CONCAT(pkw.param_name, pkw.param_stop) then
      SUBSTRING_INDEX(SUBSTRING_INDEX(marin.urldecode(td.entry_url), concat(pkw.param_name, pkw.param_stop), -1), pkw.param_stop, 1)
    end as keyword,
    case when td.entry_url REGEXP CONCAT(pmt.param_name, '=') then
      SUBSTRING_INDEX(SUBSTRING_INDEX(td.entry_url, concat(pmt.param_name, '='), -1), pmt.param_stop, 1)
    when td.entry_url REGEXP CONCAT(pmt.param_name, pmt.param_stop) then
      SUBSTRING_INDEX(SUBSTRING_INDEX(td.entry_url, concat(pmt.param_name, pmt.param_stop), -1), pmt.param_stop, 1)
    end as match_type,
    case when td.entry_url REGEXP CONCAT(pdv.param_name, pdv.param_separator) then
      SUBSTRING_INDEX(SUBSTRING_INDEX(td.entry_url, concat(pdv.param_name, pdv.param_separator), -1), pdv.param_stop, 1)
    end as device,
    case when LOWER(td.entry_url) REGEXP CONCAT(coalesce(device_delimiter.setting_value, '_'), 'd') then
      SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(LOWER(td.entry_url), concat(mkwid.param_name, '='), -1), mkwid.param_stop, 1), concat(coalesce(device_delimiter.setting_value, '_'), 'd'), -1)
    end as device_combined,
    COALESCE(IF(td.entry_url REGEXP 'currency=', SUBSTRING_INDEX(SUBSTRING_INDEX(td.entry_url, 'currency=', -1), '&', 1), NULL), clients.currency) as currency
  FROM
  (
    SELECT
      DATE(CONVERT_TZ(conv.receive_time, 'GMT', 'EST')) AS DAY,
      conv.uuid,
      SUBSTRING_INDEX(conv.order_type, '|', 1) as order_type,
      IF (conv.order_id IS NOT NULL, conv.order_id, CONCAT(conv.uuid, conv.receive_time)) AS unique_id,
      conv.order_id,
      MIN(conv.receive_time) AS conversion_time,
      MAX(clicks.receive_time) AS last_click_time,
      MIN(clicks.receive_time) AS first_click_time
    FROM marin_olap_staging.tracker_data_31163 conv
      JOIN marin.clients c ON c.client_id = 31163
      JOIN marin.conversion_types ct on ct.client_id = 31163 and conv.order_type like concat('%',ct.user_id,'%') 
      LEFT JOIN marin_olap_staging.tracker_data_31163 clicks ON 
      	conv.uuid = clicks.uuid 
      	and clicks.action_id = 1 
      	and (clicks.receive_time >= SUBDATE(conv.receive_time, INTERVAL c.conversion_window DAY) or clicks.receive_time is null)
      	and (clicks.receive_time <= conv.receive_time or clicks.receive_time is null)
    
    WHERE conv.action_id = 2
      AND conv.receive_time BETWEEN CONVERT_TZ('2017-10-15', 'EST', 'GMT')
      AND CONVERT_TZ('2018-01-01','EST', 'GMT') + INTERVAL 1 DAY
    GROUP BY
      DATE(CONVERT_TZ(conv.receive_time, 'GMT', 'EST')),
      SUBSTRING_INDEX(conv.order_type, '|', 1),
      IF (conv.order_id IS NOT NULL, conv.order_id, CONCAT(conv.uuid, conv.receive_time)),
      conv.order_id
  ) deduped
    JOIN marin.clients ON client_id = 31163
    JOIN marin_olap_staging.tracker_data_31163 td ON deduped.uuid = td.uuid
    JOIN marin.client_keyword_params mkwid on clients.client_id = mkwid.client_id and mkwid.search_order = 0
    JOIN marin.client_creative_params pcrid on clients.client_id = pcrid.client_id and pcrid.search_order = 0
    LEFT JOIN marin.application_settings device_delimiter ON clients.client_id = device_delimiter.client_id AND device_delimiter.setting = 'TRACKING_ID_DELIM'
    LEFT JOIN marin.client_keyword_text_params pkw on clients.client_id = pkw.client_id and pkw.search_order = 0
    LEFT JOIN marin.client_match_type_params pmt on clients.client_id = pmt.client_id and pmt.search_order = 0
    LEFT JOIN marin.client_tracking_params pdv on clients.client_id = pdv.client_id and pdv.search_order = 0
WHERE ((td.action_id = 1 and td.receive_time <= last_click_time AND td.receive_time >= first_click_time) OR
      (td.action_id = 2 and td.receive_time = deduped.conversion_time))
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28
) parsed
  LEFT JOIN marin.tracking_values tv ON tv.value = parsed.mkwid and tv.client_id in (select client_id from marin.clients where customer_id = parsed.customer_id and status = 'ACTIVE')
  LEFT JOIN marin.publisher_creatives as pcr_ext on pcr_ext.ext_id = parsed.pcrid and parsed.pcrid NOT REGEXP '{.+}' and parsed.pcrid NOT REGEXP '%7B.+%7D'
  LEFT JOIN marin.publisher_creative_old_ext_ids as pcr_old_ext_ids on pcr_old_ext_ids.old_ext_id = parsed.pcrid and parsed.pcrid NOT REGEXP '{.+}' and parsed.pcrid NOT REGEXP '%7B.+%7D'
  LEFT JOIN marin.publisher_creatives pcr_from_old_ext ON pcr_from_old_ext.creative_id = pcr_old_ext_ids.publisher_creative_id
  LEFT JOIN marin.publisher_creatives pcr ON pcr.creative_id = IF(parsed.creative_id > 0, parsed.creative_id, IF(tv.trackable_type = 'PublisherCreative', tv.trackable_id, NULL))
  LEFT JOIN marin.keyword_instances ki ON ki.id = IF(parsed.keyword_instance_id > 0, parsed.keyword_instance_id, IF(tv.trackable_type = 'KeywordInstance', tv.trackable_id, 'NOMATCH')) -- and ki.publisher_group_id = COALESCE(pcr_ext.publisher_group_id, pcr.publisher_group_id)
  LEFT JOIN marin.product_targets pt ON pt.product_target_id = IF(tv.trackable_type = 'ProductTarget', tv.trackable_id, NULL)
  LEFT JOIN marin.placements pl ON pl.placement_id = IF(tv.trackable_type = 'Placement', tv.trackable_id, NULL)
  LEFT JOIN marin.publisher_groups pg ON pg.publisher_group_id = COALESCE(ki.publisher_group_id, pcr_ext.publisher_group_id, pcr_from_old_ext.publisher_group_id, pcr.publisher_group_id, pt.publisher_group_id, pl.publisher_group_id)
  LEFT JOIN marin.publisher_campaigns pc ON pg.publisher_campaign_id = pc.publisher_campaign_id
  LEFT JOIN marin.publisher_client_accounts pca ON pc.client_account_id = pca.client_account_id
  LEFT JOIN marin.publisher_accounts pa ON pca.publisher_account_id = pa.publisher_account_id
  LEFT JOIN marin.clients c ON pa.client_id = c.client_id and c.customer_id = parsed.customer_id and c.status = 'ACTIVE'
  LEFT JOIN marin.conversion_types ct on c.client_id = ct.client_id and SUBSTRING_INDEX(parsed.order_type, '|', 1) = ct.user_id and ct.attribution_date = 'CONVERSION' and ct.status = 'ACTIVE'
  LEFT JOIN marin.publishers p ON pa.publisher_id = p.publisher_id
  
  LEFT JOIN marin.vo_meta_categories dim1_status ON dim1_status.name = 'dim1' and dim1_status.client_id = tv.client_id
  LEFT JOIN marin.vo_meta_data dim1_account on dim1_account.md_key = concat('usermd:', dim1_status.vmc_id) AND dim1_account.publisher_object_type = 'PublisherClientAccount' and dim1_account.publisher_object_id = pca.client_account_id
  LEFT JOIN marin.vo_meta_data dim1_campaign on dim1_campaign.md_key = concat('usermd:', dim1_status.vmc_id) AND dim1_campaign.publisher_object_type = 'PublisherCampaign' and dim1_campaign.publisher_object_id = pc.publisher_campaign_id
  LEFT JOIN marin.vo_meta_data dim1_group on dim1_group.md_key = concat('usermd:', dim1_status.vmc_id) AND dim1_group.publisher_object_type = 'PublisherGroup' and dim1_group.publisher_object_id = pg.publisher_group_id
  LEFT JOIN marin.vo_meta_data dim1_keyword on dim1_keyword.md_key = concat('usermd:', dim1_status.vmc_id) AND dim1_keyword.publisher_object_type = 'KeywordInstance' and dim1_keyword.publisher_object_id = IF(tv.trackable_type = 'KeywordInstance', tv.trackable_id, NULL)
  LEFT JOIN marin.vo_meta_data dim1_creative on dim1_creative.md_key = concat('usermd:', dim1_status.vmc_id) AND dim1_creative.publisher_object_type = 'PublisherCreative' and dim1_creative.publisher_object_id = IF(tv.trackable_type = 'PublisherCreative', tv.trackable_id, NULL)
  
  LEFT JOIN marin.vo_meta_categories dim2_status ON dim2_status.name = 'dim2' and dim2_status.client_id = tv.client_id
  LEFT JOIN marin.vo_meta_data dim2_account on dim2_account.md_key = concat('usermd:', dim2_status.vmc_id) AND dim2_account.publisher_object_type = 'PublisherClientAccount' and dim2_account.publisher_object_id = pca.client_account_id
  LEFT JOIN marin.vo_meta_data dim2_campaign on dim2_campaign.md_key = concat('usermd:', dim2_status.vmc_id) AND dim2_campaign.publisher_object_type = 'PublisherCampaign' and dim2_campaign.publisher_object_id = pc.publisher_campaign_id
  LEFT JOIN marin.vo_meta_data dim2_group on dim2_group.md_key = concat('usermd:', dim2_status.vmc_id) AND dim2_group.publisher_object_type = 'PublisherGroup' and dim2_group.publisher_object_id = pg.publisher_group_id
  LEFT JOIN marin.vo_meta_data dim2_keyword on dim2_keyword.md_key = concat('usermd:', dim2_status.vmc_id) AND dim2_keyword.publisher_object_type = 'KeywordInstance' and dim2_keyword.publisher_object_id = IF(tv.trackable_type = 'KeywordInstance', tv.trackable_id, NULL)
  LEFT JOIN marin.vo_meta_data dim2_creative on dim2_creative.md_key = concat('usermd:', dim2_status.vmc_id) AND dim2_creative.publisher_object_type = 'PublisherCreative' and dim2_creative.publisher_object_id = IF(tv.trackable_type = 'PublisherCreative', tv.trackable_id, NULL)
  

WHERE (ki.id IS NULL OR ki.publisher_group_id = COALESCE(pcr_ext.publisher_group_id, pcr_from_old_ext.publisher_group_id, pcr.publisher_group_id))
ORDER BY uuid, receive_time desc, action_id = 1

INTO OUTFILE '/usr/local/acdc/etl/tracker_31163_oct15_nov15.csv'
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
;
