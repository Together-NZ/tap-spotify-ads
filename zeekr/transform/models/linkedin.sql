{{ config(
  materialized='table',
) }}
SELECT SUM(clicks) AS clicks, SUM(impressions) as impressions, advertiser_account_id,
campaign_name,campaign_descr,ad_format,media_format,ad_format_detail,publisher,audience_name,SUM(media_cost) AS media_cost, SUM(video_25_completion) as video_25_completion, SUM(video_50_completion) AS video_50_completion, SUM(video_75_completion) AS video_75_completion, SUM(video_completion) AS video_completion,SUM(video_views) AS video_views,SUM(likes) AS likes, SUM(comments) AS comments,SUM(commentLikes) AS comment_like,SUM(follows) AS follows,SUM(totalEngagements) as total_engagements,creative_name,creative_descr,date


date FROM `together-internal.linkedin_transformed.linkedin` where  advertiser_account_id = '516476083' group by date,campaign_name,media_format,campaign_descr,ad_format,ad_format_detail,publisher,audience_name,creative_name,creative_descr,advertiser_account_id