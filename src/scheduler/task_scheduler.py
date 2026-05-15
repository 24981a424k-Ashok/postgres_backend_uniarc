import os
import random
import json
import asyncio
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any
from apscheduler.schedulers.background import BackgroundScheduler
from sqlalchemy import text
from sqlalchemy.orm import Session
from loguru import logger

from src.config.settings import SCHEDULE_TIME
from src.database.models import SessionLocal, RawNews, VerifiedNews
from src.collectors.news_api import NewsCollector
from src.collectors.twitter_collector import TwitterCollector
from src.verification.verifier import VerificationEngine
from src.collectors.social_media_collector import SocialMediaCollector
from src.utils.translator import NewsTranslator
from src.analysis.llm_analyzer import LLMAnalyzer
from src.digest.generator import DigestGenerator
from src.delivery.notifications import NotificationManager
from src.delivery.sms_notifier import SmsNotifier
from src.config.firebase_config import initialize_firebase

async def run_news_cycle():
    logger.info("Starting Daily News Cycle...")
    
    # Update global monitor in main.py if possible
    try:
        import main
        main.LAST_CYCLE_RUN = datetime.utcnow().isoformat()
    except:
        pass

    initialize_firebase()
    
    try:
        # 1. Collect
        logger.info("Step 1: Parallel Collection")
        
        async def fetch_api():
            try:
                collector = NewsCollector()
                return await asyncio.to_thread(collector.fetch_recent_news)
            except Exception as e:
                logger.error(f"NewsAPI Collector failed: {e}")
                return 0

        async def fetch_rss():
            try:
                from src.collectors.rss_collector import RSSCollector
                collector = RSSCollector()
                return await asyncio.to_thread(collector.fetch_recent_news)
            except Exception as e:
                logger.error(f"RSS Collector failed: {e}")
                return 0

        async def fetch_twitter():
            try:
                collector = TwitterCollector()
                return await asyncio.to_thread(collector.fetch_top_updates)
            except Exception as e:
                logger.error(f"Twitter Collector failed: {e}")
                return 0

        async def fetch_trending():
            try:
                collector = SocialMediaCollector()
                return await asyncio.to_thread(collector.fetch_trending_india)
            except Exception as e:
                logger.error(f"Social Media Collector failed: {e}")
                return 0

        async def fetch_gnews():
            try:
                from src.collectors.gnews_collector import GNewsCollector
                collector = GNewsCollector()
                return await asyncio.to_thread(collector.fetch_country_news)
            except Exception as e:
                logger.error(f"GNews Collector failed: {e}")
                return 0

        # Run all collectors in parallel
        results = await asyncio.gather(
            fetch_api(),
            fetch_rss(),
            fetch_twitter(),
            fetch_trending(),
            fetch_gnews()
        )
        
        api_count, rss_count, twitter_result, trending_result, gnews_count = results
        
        # Safe count extraction
        t_count = twitter_result.get('new', 0) if isinstance(twitter_result, dict) else (twitter_result or 0)
        s_count = trending_result.get('new', 0) if isinstance(trending_result, dict) else (trending_result or 0)
        
        total_count = api_count + rss_count + t_count + s_count + gnews_count
        logger.info(f"✅ Collection complete. Total new articles: {total_count}")
        
        # 2. Verify
        logger.info("Step 2: Verification")
        with SessionLocal() as db:
            if total_count == 0 and db.query(RawNews).count() == 0:
                logger.warning("No news collected and DB is empty. Aborting cycle.")
                return

            verifier = VerificationEngine()
            unprocessed = db.query(RawNews).filter(RawNews.processed == False).all()
            verified_count = verifier.verify_batch(db, [n.id for n in unprocessed])
            logger.info(f"Verified {verified_count} articles.")

        # 3. Instant Dashboard Refresh
        logger.info("Step 3: Instant Dashboard Refresh...")
        with SessionLocal() as db:
            generator = DigestGenerator()
            await generator.create_daily_digest(db)
        
        # 4. Analyze (Deep Intelligence)
        logger.info("Step 4: AI Analysis")
        with SessionLocal() as db:
            analyzer = LLMAnalyzer()
            unanalyzed = db.query(VerifiedNews).filter(VerifiedNews.impact_score == None).all()
            
            if unanalyzed:
                sports_articles = []
                other_articles = []
                
                for n in unanalyzed:
                    is_likely_sports = False
                    if n.raw_news and n.raw_news.source_id:
                        sid = n.raw_news.source_id.lower()
                        if any(k in sid for k in ["sport", "espn", "football", "cricket"]):
                            is_likely_sports = True
                    
                    if not is_likely_sports and n.title:
                        title_lower = n.title.lower()
                        if any(k in title_lower for k in ["match", "tournament", "scored", "wicket", "stadium", "athlete", "cricket", "football", "olympic", "fifa", "premier league"]):
                            is_likely_sports = True
                    
                    article_data = {
                        "title": n.title, 
                        "content": n.content,
                        "source_name": n.raw_news.source_name if n.raw_news else "Source"
                    }
                    
                    if is_likely_sports:
                        sports_articles.append((n, article_data))
                    else:
                        other_articles.append((n, article_data))
                
                def apply_analysis_to_news(news, result):
                    news.analysis = result
                    news.summary_bullets = result.get("summary_bullets", [])
                    news.why_it_matters = str(result.get("why_it_matters", ""))
                    who = result.get("who_is_affected", "")
                    if isinstance(who, dict): news.who_is_affected = json.dumps(who)
                    else: news.who_is_affected = str(who)
                    news.short_term_impact = str(result.get("short_term_impact", ""))
                    news.long_term_impact = str(result.get("long_term_impact", ""))
                    news.sentiment = str(result.get("sentiment", "Neutral"))
                    news.impact_tags = result.get("impact_tags", [])
                    news.bias_rating = str(result.get("bias_rating", "Neutral"))
                    news.impact_score = int(result.get("impact_score", 5))
                    
                    if result.get("category") == "Sports" or news.category == "Sports":
                        is_major_event = any(k in (news.title or "").lower() for k in ["olympic", "fifa", "world cup", "championship", "final"])
                        if not is_major_event and news.impact_score > 6: news.impact_score = 6
                    news.country = result.get("country") or result.get("primary_geography") or (news.raw_news.country if news.raw_news else None)
                    news.category = result.get("category", "General")

                if sports_articles:
                    sports_results = await analyzer.analyze_batch([a[1] for a in sports_articles], is_sports=True)
                    for (news, _), result in zip(sports_articles, sports_results):
                        apply_analysis_to_news(news, result)
                        news.category = "Sports"
                
                if other_articles:
                    other_results = await analyzer.analyze_batch([a[1] for a in other_articles], is_sports=False)
                    for (news, _), result in zip(other_articles, other_results):
                        apply_analysis_to_news(news, result)
                
                db.commit()
                logger.info(f"AI Intelligence applied to {len(unanalyzed)} articles.")
            
            # Cleanup
            db.execute(text("DELETE FROM raw_news WHERE processed = 1 AND collected_at < :cutoff"), 
                      {"cutoff": datetime.utcnow() - timedelta(days=2)})
            db.commit()

        # 5. Final Digest Update
        logger.info("Step 5: Updating Intelligence Dashboard...")
        with SessionLocal() as db:
            await generator.create_daily_digest(db)

        # 6. Deliver
        logger.info("Step 6: Delivering Intelligence Notifications")
        with SessionLocal() as db:
            newly_analyzed = db.query(VerifiedNews).filter(
                VerifiedNews.impact_score >= 9,
                VerifiedNews.created_at >= (datetime.utcnow() - timedelta(minutes=60))
            ).all()
            for item in newly_analyzed:
                await SmsNotifier.broadcast_breaking_news(db, item)
            await check_topic_tracking(db)

        # Update last run time on success
        with SessionLocal() as db:
            from src.database.models import SystemConfig
            entry = db.query(SystemConfig).filter(SystemConfig.config_key == "last_news_cycle_run").first()
            if not entry:
                entry = SystemConfig(config_key="last_news_cycle_run")
                db.add(entry)
            entry.config_value = datetime.utcnow().isoformat()
            db.commit()
            logger.info(f"Last successful run time updated: {entry.config_value}")

        # 7. Pre-Translate (Optimal Solution for zero lag)
        logger.info("Step 7: Background Pre-translation (Top 5 Languages)")
        await pre_translate_top_stories()

    except Exception as e:
        logger.error(f"Error in news cycle: {e}")
        # Final emergency update of timestamp to prevent infinite retry loops if one article is toxic
        with SessionLocal() as db_cfg:
            try:
                from src.database.models import SystemConfig
                entry = db_cfg.query(SystemConfig).filter(SystemConfig.config_key == "last_news_cycle_run").first()
                if not entry:
                    entry = SystemConfig(config_key="last_news_cycle_run")
                    db_cfg.add(entry)
                entry.config_value = datetime.utcnow().isoformat()
                db_cfg.commit()
            except:
                db_cfg.rollback()

        logger.info("--------------------------------------------------")
        logger.info("FINISHED WITH ERRORS | NEXT CYCLE IN 15 MINUTES")
        logger.info("--------------------------------------------------")

async def check_topic_tracking(db: Session):
    """Check for new articles matching tracked topics and notify users."""
    try:
        from src.database.models import TopicTracking, VerifiedNews, User, TrackNotification
        from src.delivery.notifications import NotificationManager
        from datetime import datetime, timedelta
        
        # Look for tracks created or updated recently
        # In a real system, we'd track 'last_notified_at'
        # For now, look for news from the last hour that matches active tracks
        one_hour_ago = datetime.utcnow() - timedelta(hours=1)
        new_articles = db.query(VerifiedNews).filter(VerifiedNews.created_at > one_hour_ago).all()
        
        if not new_articles:
            return

        tracks = db.query(TopicTracking).filter(
            TopicTracking.notify_sms == True,
            TopicTracking.expires_at > datetime.utcnow()
        ).all()
        
        for track in tracks:
            user = track.user
            if not user or not user.phone:
                continue
            
            for article in new_articles:
                # Basic keyword matching
                match = False
                for kw in (track.topic_keywords or []):
                    if kw.lower() in article.title.lower() or kw.lower() in (article.category or "").lower():
                        match = True
                        break
                
                if match:
                    # CHECK FOR DUPLICATE
                    already_notified = db.query(TrackNotification).filter(
                        TrackNotification.user_id == user.id,
                        TrackNotification.news_id == article.id
                    ).first()
                    
                    if not already_notified:
                        logger.info(f"Topic Match Found! Notifying {user.phone} for '{article.title}'")
                        await NotificationManager.send_sms(
                            user.phone, 
                            f"Tracked Intelligence: '{article.title}' matches your search. Read more: {article.url}"
                        )
                        # RECORD NOTIFICATION
                        db.add(TrackNotification(user_id=user.id, news_id=article.id))
                        db.commit()
                    
    except Exception as e:
        logger.error(f"Error in topic tracking check: {e}")


async def pre_translate_top_stories():
    """
    Perform background translation for the top 10 stories into major languages.
    This ensures that when users open the app in these languages, the content is INSTANT.
    """
    try:
        from src.database.models import DailyDigest, SessionLocal
        from src.utils.translator import NewsTranslator
        
        target_langs = ["Hindi", "Telugu", "Tamil", "Kannada", "Malayalam"]
        translator = NewsTranslator()
        
        with SessionLocal() as db:
            latest = db.query(DailyDigest).filter(DailyDigest.is_published == True).order_by(DailyDigest.date.desc()).first()
            if not latest or not latest.content_json:
                logger.warning("No digest found for pre-translation.")
                return

            top_stories = latest.content_json.get("top_stories", [])[:10]
            if not top_stories:
                logger.warning("Top stories empty, skipping pre-translation.")
                return
            
            # Prepare data structure for translator
            node_data = {"stories": top_stories}
            
            logger.info(f"Pre-translating {len(top_stories)} stories into {len(target_langs)} languages...")
            
            # Run translations sequentially to avoid overwhelming API rate limits
            for lang in target_langs:
                try:
                    logger.info(f"Pre-translating to {lang}...")
                    # translate_node_bulk handles its own internal caching and DB updates
                    await translator.translate_node_bulk(node_data, lang)
                    # Small rest between languages
                    await asyncio.sleep(2) 
                except Exception as lang_err:
                    logger.error(f"Pre-translation failed for {lang}: {lang_err}")
                    
            logger.info("✅ Background Pre-translation complete.")

    except Exception as e:
        logger.error(f"Global Pre-translation error: {e}")


async def run_twitter_only_cycle():
    """Lightweight cycle just for Twitter and Dashboard updates."""
    logger.info("Starting Lightweight Twitter Cycle...")
    initialize_firebase()
    db = SessionLocal()
    try:
        # 1. Collect Twitter
        twitter_collector = TwitterCollector()
        twitter_result = twitter_collector.fetch_top_updates()
        twitter_count = twitter_result.get('new', 0) if isinstance(twitter_result, dict) else (twitter_result or 0)
        logger.info(f"Collected {twitter_count} tweets.")

        # 2. Force Digest Generation (This also promotes raw tweets to verified in our patched generator)
        generator = DigestGenerator()
        await generator.create_daily_digest(db)
        logger.info("Digest updated with fresh Twitter intelligence.")

    except Exception as e:
        logger.error(f"Error in twitter cycle: {e}")
    finally:
        db.close()
        logger.info("Twitter Cycle Completed.")

def start_scheduler():
    scheduler = BackgroundScheduler()
    
    # Run every 15 minutes (Balanced Update Cycle)
    from datetime import datetime, timedelta
    # Increase delay to 10 seconds to allow web server to fully stabilize and pass health checks on HF
    run_date = datetime.now() + timedelta(seconds=10)
    
    # helper to run async in background
    def _run_async_cycle():
        import asyncio
        from datetime import datetime, timedelta
        run_time = datetime.now()
        next_run = run_time + timedelta(minutes=15)
        logger.info(f"--- 🚀 NEWS CYCLE START: {run_time.strftime('%H:%M:%S')} (Next: {next_run.strftime('%H:%M:%S')}) ---")
        
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(run_news_cycle())
        loop.close()
        
        end_time = datetime.now()
        duration = (end_time - run_time).total_seconds()
        
        logger.info(f"--- ✅ NEWS CYCLE SUCCESSFUL (Took {duration:.1f} seconds) ---")
        logger.info(f"--- 🕰️  STANDBY: Next automated update at {next_run.strftime('%H:%M:%S')} ---")

    def _run_async_twitter():
        import asyncio
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(run_twitter_only_cycle())
        loop.close()

    def _run_data_retention():
        try:
            from src.utils.data_retention import prune_old_data
            logger.info("--- 🧹 DATA RETENTION START: Cleaning old articles (>10 days) ---")
            prune_old_data(days=10)
            logger.info("--- ✅ DATA RETENTION SUCCESSFUL ---")
        except Exception as e:
            logger.error(f"Data retention failed: {e}")

    # FULL NEWS CYCLE (Every 3 minutes for high-speed updates)
    scheduler.add_job(
        _run_async_cycle, 
        'interval', 
        minutes=15, 
        next_run_time=run_date, 
        id='full_news_cycle',
        max_instances=3, 
        misfire_grace_time=3600,
        coalesce=True
    )
    
    # Daily Newspaper Update
    scheduler.add_job(
        _run_async_cycle, 
        'cron', 
        hour=6, 
        minute=30, 
        timezone='Asia/Kolkata',
        id='daily_newspaper_update',
        max_instances=3,
        misfire_grace_time=3600,
        coalesce=True
    )

    # DATA RETENTION (Every 24 hours at 3 AM)
    scheduler.add_job(
        _run_data_retention,
        'cron',
        hour=3,
        minute=0,
        id='data_retention_job',
        misfire_grace_time=3600,
        coalesce=True
    )
    
    scheduler.start()
    return scheduler
