import feedparser
import logging
import os
import random
import asyncio
import json
from datetime import datetime, timedelta
from dateutil import parser
from typing import List, Dict, Any
from src.config import settings
from src.config.settings import OPENAI_API_KEY
from src.database.models import SessionLocal, VerifiedNews
from sqlalchemy import or_

logger = logging.getLogger(__name__)

UNIVERSE_SOURCES = {
    "BBC News": "http://feeds.bbci.co.uk/news/world/rss.xml",
    "CNN": "http://rss.cnn.com/rss/edition_world.rss",
    "Reuters": "https://news.google.com/rss/search?q=when:3d+source:Reuters&hl=en-US&gl=US&ceid=US:en",
    "Associated Press": "https://news.google.com/rss/search?q=when:3d+source:Associated+Press&hl=en-US&gl=US&ceid=US:en",
    "Al Jazeera": "https://www.aljazeera.com/xml/rss/all.xml",
    "The New York Times": "https://rss.nytimes.com/services/xml/rss/nyt/World.xml",
    "The Washington Post": "https://news.google.com/rss/search?q=when:3d+source:Washington+Post&hl=en-US&gl=US&ceid=US:en",
    "The Guardian": "https://www.theguardian.com/world/rss",
    "Times of India": "https://timesofindia.indiatimes.com/rssfeeds/296589292.cms",
    "Euronews": "https://www.euronews.com/rss?level=vertical&name=world"
}

COUNTRY_ALIASES = {
    "india": ["india", "indian", "modi", "delhi", "mumbai", "bengaluru"],
    "usa": ["usa", "us ", "united states", "america", "washington", "trump", "biden", "new york"],
    "united states": ["usa", "us ", "united states", "america", "washington", "trump", "biden", "new york"],
    "uk": ["uk ", "united kingdom", "britain", "british", "london", "starmer", "sunak"],
    "united kingdom": ["uk ", "united kingdom", "britain", "british", "london", "starmer", "sunak"],
    "china": ["china", "chinese", "beijing", "xi jinping", "shanghai", "hong kong", "taiwan"],
    "russia": ["russia", "putin", "moscow", "kremlin", "ukraine"],
    "japan": ["japan", "tokyo", "japanese", "osaka"],
    "uae": ["uae", "united arab emirates", "dubai", "abu dhabi", "sharjah", "emirates"],
    "africa": ["africa", "african", "nigeria", "south africa", "kenya", "egypt", "ethiopia", "ghana", "nairobi", "cairo", "lagos", "johannesburg"],
    "europe": ["europe", "european", "eu ", "brussels", "germany", "france", "italy", "spain", "berlin", "paris", "rome", "madrid"],
    "middle east": ["middle east", "israel", "gaza", "palestine", "iran", "saudi arabia", "uae", "dubai", "tehran", "tel aviv", "riyadh"],
    "france": ["france", "french", "paris", "macron"],
    "germany": ["germany", "german", "berlin", "scholz"],
    "canada": ["canada", "canadian", "trudeau", "toronto", "ottawa"]
}

class UniverseCollector:
    def __init__(self):
        self.sources = UNIVERSE_SOURCES
        # Use simple pool from settings
        self.api_keys = settings.OPENAI_API_KEYS
        if not self.api_keys:
            self.api_keys = [OPENAI_API_KEY]


    async def fetch_country_news(self, country: str) -> Dict[str, Any]:
        """
        Fetch news from the 10 sources and categorize/analyze for the full dashboard experience.
        """
        all_articles = []
        
        # 1. Prepare sources
        dynamic_sources = self.sources.copy()
        search_query = country.replace(" ", "+")
        dynamic_sources["Global Search"] = f"https://news.google.com/rss/search?q={search_query}&hl=en-US&gl=US&ceid=US:en"
        dynamic_sources["Video Feed"] = f"https://news.google.com/rss/search?q={search_query}+video&hl=en-US&gl=US&ceid=US:en"
        
        # 1. Fetch from RSS sources in parallel
        tasks = [self._fetch_from_rss(name, url) for name, url in dynamic_sources.items()]
        results = await asyncio.gather(*tasks)
        
        for name, res in zip(dynamic_sources.keys(), results):
            all_articles.extend(res)
            
        # 2. Filter articles that might be relevant to the country
        relevant_articles = []
        country_lower = country.lower().strip()
        search_terms = COUNTRY_ALIASES.get(country_lower, [country_lower])
        
        for art in all_articles:
            text = (art['title'] + " " + (art['content'] or "")).lower()
            if any(term in text for term in search_terms):
                relevant_articles.append(art)
        
        if not relevant_articles:
            return {"top_stories": [], "breaking_news": [], "videos": [], "newspaper_summary": "", "newspapers": []}

        # 3. Analyze with OpenAI (batch processing) - Reduced Batch to 15 for better speed
        analyzed_news = await self._analyze_with_rotation(relevant_articles[:15], country)
        
        # 4. Fallback Logic
        if not analyzed_news and relevant_articles:
            analyzed_news = self._generate_hybrid_fallback(relevant_articles, country)
            
        # 5. Discover Regional Newspapers
        newspapers = await self._fetch_regional_newspapers(country)

        # 6. Merge with Local Verified News (Deduplicated)
        db = SessionLocal()
        try:
            local_verified = db.query(VerifiedNews).filter(
                or_(
                    VerifiedNews.country.ilike(f"%{country}%"),
                    VerifiedNews.title.ilike(f"%{country}%")
                )
            ).order_by(VerifiedNews.impact_score.desc()).limit(10).all()
            
            seen_titles = {a['news_headline'].lower() for a in analyzed_news}
            for v in local_verified:
                if v.title.lower() not in seen_titles:
                    analyzed_news.append({
                        "source_name": v.source_name or "Global Intel",
                        "news_headline": v.title,
                        "intelligence_summary": v.summary_bullets or [v.why_it_matters],
                        "why_it_matters_to_" + country: v.why_it_matters,
                        "bias_rating": v.bias_rating or "Neutral",
                        "impact_score": v.impact_score or 7,
                        "url": f"/article/{v.id}",
                        "image_url": getattr(v, 'url_to_image', None) or v.analysis.get('image_url') if v.analysis else None,
                        "audio_url": getattr(v, 'audio_url', None),
                        "time_ago": "Verified Node"
                    })
        finally:
            db.close()

        # 7. Structure into Dashboard Categories - INCREASED Quantities
        structured_data = {
            "top_stories": analyzed_news[5:20] if len(analyzed_news) > 20 else analyzed_news[5:], 
            "breaking_news": analyzed_news[:5], 
            "videos": self._extract_video_candidates(analyzed_news + relevant_articles, country),
            "newspaper_summary": await self._generate_newspaper_summary(analyzed_news, country),
            "newspapers": newspapers
        }
        
        return structured_data

    async def _fetch_regional_newspapers(self, country: str) -> List[Dict[str, Any]]:
        """Dynamically finds major publications for the searched country."""
        papers = []
        try:
            # Targeted search for newspapers in that country
            url = f"https://news.google.com/rss/search?q=top+newspapers+in+{country.replace(' ', '+')}&hl=en-US&gl=US&ceid=US:en"
            feed = feedparser.parse(url)
            
            seen_sources = set()
            for entry in feed.entries[:10]:
                source = entry.get('source', {}).get('title') or entry.get('publisher') or "Local News"
                if source not in seen_sources and "Google News" not in source:
                    seen_sources.add(source)
                    # Extract a likely home URL from the link if possible, or just use the link
                    papers.append({
                        "name": source,
                        "url": entry.get('link'),
                        "color": random.choice(["#3b82f6", "#ef4444", "#10b981", "#f59e0b", "#6366f1", "#000000"])
                    })
                if len(papers) >= 6: break
                
            # If no specific papers found, use standard global ones as fallback
            if not papers:
                papers = [
                    {"name": "BBC World", "url": "https://www.bbc.com/news/world", "color": "#b91c1c"},
                    {"name": "CNN Intl", "url": "https://edition.cnn.com/", "color": "#cc0000"},
                    {"name": "Reuters", "url": "https://www.reuters.com/", "color": "#ff8000"}
                ]
            return papers
        except Exception as e:
            logger.error(f"Newspaper discovery failed for {country}: {e}")
            return []

    async def _generate_newspaper_summary(self, analysis: List[Dict[str, Any]], country: str) -> str:
        """Generates a cohesive briefing summary for the country."""
        if not analysis: return f"Stable conditions reported across {country} nodes. Strategic monitoring continues."
        
        # Use simple synthesis for speed/quota if too many calls, or use OpenAI if available
        summary_points = [a['news_headline'] for a in analysis[:5]]
        prompt = f"Summarize the current situation in {country} based on these headlines in 2-3 engaging sentences for a 'Digital Newspaper' briefing:\n" + "\n".join(summary_points)
        
        # We'll use the first available key
        for key in self.api_keys + [OPENAI_API_KEY]:
            if not key: continue
            try:
                from openai import AsyncOpenAI
                client = AsyncOpenAI(api_key=key)
                response = await client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[{"role": "user", "content": prompt}],
                    max_tokens=150,
                    timeout=10
                )
                res = response.choices[0].message.content.strip()
                await client.close()
                return res
            except:
                continue
        
        return f"Key updates from {country} include: " + ", ".join(summary_points[:2]) + ". Analysts are monitoring the evolving situation."

    def _extract_video_candidates(self, news: List[Dict[str, Any]], country: str) -> List[Dict[str, Any]]:
        """Identifies articles that likely contain video content or placeholders."""
        videos = []
        for item in news[:10]:
            if 'image_url' in item and item.get('image_url'):
                videos.append({
                    "title": item.get('news_headline') or item.get('title'),
                    "url": item.get('url'),
                    "image_url": item.get('image_url'),
                    "duration": f"{random.randint(1,5)}:{random.randint(10,59)}",
                    "source": item.get('source_name', 'Global News')
                })
        return videos[:6] # Extended to 6

    async def _fetch_from_rss(self, source_name: str, url: str) -> List[Dict[str, Any]]:
        try:
            feed = feedparser.parse(url)
            articles = []
            cutoff = datetime.utcnow() - timedelta(days=7) 
            
            for entry in feed.entries:
                pub_date = self._parse_date(entry)
                if pub_date < cutoff: continue
                
                image = self._extract_image(entry)
                articles.append({
                    "source_name": source_name,
                    "title": entry.get("title", ""),
                    "url": entry.get("link", ""),
                    "content": entry.get("summary", "") or entry.get("description", ""),
                    "published_at": pub_date.isoformat(),
                    "image_url": image
                })
            return articles
        except Exception as e:
            logger.error(f"Error fetching {source_name}: {e}")
            return []

    def _generate_hybrid_fallback(self, articles: List[Dict[str, Any]], country: str) -> List[Dict[str, Any]]:
        """Generates structured intelligence cards without using OpenAI."""
        fallbacks = []
        for art in articles[:15]: # Increased fallback pool
            impact = 6
            if any(word in art['title'].lower() for word in ['war', 'deal', 'crisis', 'attack', 'dead', 'treaty', 'protest']):
                impact = 8
            
            fallbacks.append({
                "source_name": art['source_name'],
                "news_headline": art['title'],
                "intelligence_summary": [
                    f"Reported from {art['source_name']}.",
                    "Recent updates indicate shifting regional dynamics.",
                    "Strategic interests remain under observation."
                ],
                "why_it_matters_to_" + country: f"Direct impact on {country}'s regional stability and international relations.",
                "bias_rating": "Neutral",
                "impact_score": impact,
                "url": art['url'],
                "image_url": art['image_url'],
                "time_ago": "Recently"
            })
        return fallbacks

    def _parse_date(self, entry) -> datetime:
        date_str = entry.get("published") or entry.get("updated") or entry.get("date")
        if date_str:
            try:
                return parser.parse(date_str).replace(tzinfo=None)
            except: pass
        return datetime.utcnow()

    def _extract_image(self, entry) -> str:
        if 'media_content' in entry:
            for media in entry.media_content:
                if media.get('type', '').startswith('image'):
                    return media.get('url')
        if 'links' in entry:
            for link in entry.links:
                if link.get('rel') == 'enclosure' and link.get('type', '').startswith('image'):
                    return link.get('href')
        return None

    async def search_global(self, query: str, db: SessionLocal) -> List[Dict[str, Any]]:
        """
        Hyper-fast global search using dynamic source discovery and parallel collection.
        Optimized for < 40s response time.
        """
        logger.info(f"UNIVERSE: Performing hyper-fast global search for '{query}'...")
        
        # 1. Broad dynamic source creation
        search_query = query.replace(" ", "+")
        dynamic_sources = {
            "Global Intelligence": f"https://news.google.com/rss/search?q={search_query}&hl=en-US&gl=US&ceid=US:en",
            "Contextual Nodes": f"https://news.google.com/rss/search?q={search_query}+intelligence&hl=en-US&gl=US&ceid=US:en",
            "Regional Insights": f"https://news.google.com/rss/search?q={search_query}+analysis&hl=en-US&gl=US&ceid=US:en"
        }
        
        # 2. Parallel RSS fetch
        tasks = [self._fetch_from_rss(name, url) for name, url in dynamic_sources.items()]
        rss_results = await asyncio.gather(*tasks)
        
        all_articles = []
        for res in rss_results:
            all_articles.extend(res)
            
        if not all_articles:
            return []

        # 3. High-speed Parallel Analysis (Limit to top 10 for search speed)
        # Use a very short timeout and prioritize premium keys
        analyzed = await self._analyze_with_rotation(all_articles[:10], query)
        
        # 4. Fallback if LLM is slow
        if not analyzed:
            analyzed = self._generate_hybrid_fallback(all_articles, query)
            
        return analyzed

    async def _analyze_with_rotation(self, articles: List[Dict[str, Any]], country: str) -> List[Dict[str, Any]]:
        """Analyze a batch of articles using rotated API keys with aggressive timeouts."""
        results = []
        if not articles: return []

        prompt = f"""
        Analyze these news articles for relevance to: {country}.
        Output ONLY a JSON list of objects:
        - news_headline
        - intelligence_summary (2-3 bullets)
        - why_it_matters_to_{country}
        - bias_rating
        - impact_score (1-10)

        Articles:
        {json.dumps([{ 'title': a['title'], 'content': a['content'][:250], 'source': a['source_name'] } for a in articles])}
        """
        
        all_keys = self.api_keys + [OPENAI_API_KEY]
        all_keys = [k for k in all_keys if k]
        
        # INCREASED AGGRESSION: Limit attempt count to 3 for search stability
        for i, key in enumerate(all_keys[:5]):
            try:
                from openai import AsyncOpenAI
                client = AsyncOpenAI(api_key=key)
                # REDUCED TIMEOUT: 15s for search speed
                response = await client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[
                        {"role": "system", "content": "You are a global intelligence analyst. Output ONLY JSON."},
                        {"role": "user", "content": prompt}
                    ],
                    timeout=15, 
                    temperature=0.2
                )
                raw_content = response.choices[0].message.content
                if "```json" in raw_content:
                    raw_content = raw_content.split("```json")[1].split("```")[0].strip()
                
                analysis = json.loads(raw_content)
                
                for item in analysis:
                    matching_art = next((a for a in articles if item['news_headline'].lower() in a['title'].lower() or a['title'].lower() in item['news_headline'].lower()), None)
                    if matching_art:
                        item['url'] = matching_art['url']
                        item['image_url'] = matching_art['image_url']
                        item['source_name'] = matching_art['source_name']
                        item['time_ago'] = f"{random.randint(1,48)}m ago"
                        results.append(item)
                
                await client.close()
                return results 
            except Exception as e:
                logger.warning(f"Universe Analysis API Key failure index {i}: {e}")
                continue
        
        return []

