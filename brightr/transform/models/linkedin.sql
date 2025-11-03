
{{ config(
    materialized='table',
) }}

SELECT SUM(clicks) AS clicks, SUM(impressions) as impressions, advertiser_account_id,
campaign_name,campaign_descr,ad_format,creative_name,ad_format_detail,publisher,audience_name,SUM(media_cost) AS media_cost, SUM(video_25_completion) as video_25_completion, SUM(video_50_completion) AS video_50_completion, SUM(video_75_completion) AS video_75_completion, SUM(video_completion) AS video_completion,SUM(video_views) AS video_views,SUM(likes) AS likes, SUM(comments) AS comments,SUM(commentLikes) AS comment_like,SUM(follows) AS follows,SUM(totalEngagements) as total_engagements,creative_descr,date


date FROM `together-internal.linkedin_transformed.linkedin` where  advertiser_account_id = '508801448' group by date,campaign_name,creative_name,campaign_descr,ad_format,ad_format_detail,publisher,audience_name,creative_name,creative_descr,advertiser_account_id

