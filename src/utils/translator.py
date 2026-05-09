import logging
import random
import json
import asyncio
import time
import httpx
from typing import List, Dict, Any, Union
from openai import AsyncOpenAI
from src.config import settings
from src.database.models import SessionLocal, VerifiedNews

logger = logging.getLogger(__name__)


GROQ_BASE_URL = "https://api.groq.com/openai/v1"
GROQ_MODEL = "llama-3.3-70b-versatile"
HF_NLLB_URL = "https://api-inference.huggingface.co/models/facebook/nllb-200-distilled-600M"

class NewsTranslator:
    def __init__(self):
        # 1. Gather unique non-empty keys from settings pools
        self.openai_keys = list(dict.fromkeys([k for k in settings.OPENAI_API_KEYS if k]))
        self.groq_keys = list(dict.fromkeys([k for k in settings.GROQ_API_KEYS if k]))
        
        # Combined Pool for high-speed rotation
        self.all_keys = self.openai_keys + self.groq_keys
        self.current_key_idx = 0
        
        # 2. Key Status Tracking (To prevent spamming dead/limited keys)
        self._key_status = {}
        
        # REDUCED CONCURRENCY: Higher stability
        self._concurrency_limit = asyncio.Semaphore(5) 
        
        # NLLB Language Mapping
        self.nllb_map = {
            "Telugu": "tel_Telu", "Hindi": "hin_Deva", "Tamil": "tam_Taml",
            "Kannada": "kan_Knda", "Malayalam": "mal_Mlym", "Arabic": "arb_Arab",
            "Japanese": "jpn_Jpan", "Spanish": "spa_Latn", "French": "fra_Latn",
            "German": "deu_Latn", "Russian": "rus_Cyrl", "Chinese": "zho_Hans",
            "Korean": "kor_Hang", "Portuguese": "por_Latn", "Turkish": "tur_Latn"
        }
        
        if not self.all_keys:
            logger.warning("No API keys found for NewsTranslator. Translation will be skipped.")
        else:
            logger.info(f"NewsTranslator initialized with {len(self.all_keys)} keys.")
        
        self._clients: Dict[str, AsyncOpenAI] = {}

    async def verify_all_keys(self) -> Dict[str, Any]:
        """
        Hardcore check: verifies every OpenAI and Groq key in the pool.
        Returns a detailed report of active/dead keys.
        """
        logger.info("Starting hardcore key health verification...")
        results = {"active": [], "dead": [], "limited": []}
        
        async def check_single_key(key):
            client, provider = self._get_client_by_key(key)
            model = "gpt-4o-mini" if provider == "OpenAI" else GROQ_MODEL
            try:
                # Minimum prompt to save tokens
                await client.chat.completions.create(
                    model=model,
                    messages=[{"role": "user", "content": "hi"}],
                    max_tokens=5,
                    timeout=10
                )
                self._key_status[key] = {"status": "active", "retry_after": 0}
                return key, provider, "active"
            except Exception as e:
                error_msg = str(e).lower()
                is_quota = any(word in error_msg for word in ["quota", "insufficient", "spend", "limit"])
                is_rate = "429" in error_msg or "rate_limit" in error_msg
                
                status = "dead" if is_quota else ("limited" if is_rate else "error")
                self._mark_key_limited(key, is_dead=(status == "dead"))
                return key, provider, status

        tasks = [check_single_key(k) for k in self.all_keys]
        checks = await asyncio.gather(*tasks)
        
        for key, provider, status in checks:
            short_key = f"{key[:6]}...{key[-4:]}"
            if status == "active":
                results["active"].append(f"{provider}: {short_key}")
            elif status == "dead":
                results["dead"].append(f"{provider}: {short_key}")
            else:
                results["limited"].append(f"{provider}: {short_key}")
        
        logger.info(f"Verification complete: {len(results['active'])} active, {len(results['dead'])} dead, {len(results['limited'])} limited.")
        return results

    async def translate_nllb(self, text: str, target_lang: str) -> str:
        """
        Layer 3: Emergency Fallback using NLLB via Hugging Face.
        Zero cost, infinite availability (within rate limits).
        """
        if not settings.HUGGINGFACE_API_KEY:
            return text
            
        nllb_code = self.nllb_map.get(target_lang)
        if not nllb_code:
            # Fallback to English if language not supported by NLLB
            return text

        headers = {"Authorization": f"Bearer {settings.HUGGINGFACE_API_KEY}"}
        payload = {
            "inputs": text,
            "parameters": {"src_lang": "eng_Latn", "tgt_lang": nllb_code}
        }

        try:
            async with httpx.AsyncClient(timeout=30) as client:
                response = await client.post(HF_NLLB_URL, headers=headers, json=payload)
                if response.status_code == 200:
                    result = response.json()
                    if isinstance(result, list) and len(result) > 0:
                        return result[0].get("translation_text", text)
                elif response.status_code == 503: # Model loading
                    await asyncio.sleep(2)
                    return await self.translate_nllb(text, target_lang)
                
                logger.warning(f"NLLB failed with status {response.status_code}: {response.text}")
        except Exception as e:
            logger.error(f"NLLB error: {e}")
        
        return text

    def _get_best_key(self):
        """Selects the next available key from the rotation pool, prioritizing premium keys."""
        now = time.time()
        
        premium_openai = [settings.OPENAI_KEY_1, settings.OPENAI_KEY_2, settings.OPENAI_KEY_3]
        premium_groq = [settings.GROQ_KEY_1, settings.GROQ_KEY_2]
        
        all_others = [k for k in self.all_keys if k not in premium_openai and k not in premium_groq]
        priority_queue = [k for k in premium_openai if k] + [k for k in premium_groq if k] + all_others
        
        for key in priority_queue:
            status = self._key_status.get(key, {"status": "active", "retry_after": 0})
            if status["status"] == "dead": continue
            if status["status"] == "cooled_down":
                if now < status["retry_after"]: continue
                else: self._key_status[key] = {"status": "active", "retry_after": 0}
            
            return key, priority_queue.index(key)
        return None, None

    def _clean_json(self, text_content):
        """Search for and extract valid JSON from a mixed-text response."""
        if not text_content: return None
        try:
            clean = text_content.strip()
            if "```json" in clean:
                clean = clean.split("```json")[1].split("```")[0].strip()
            elif "```" in clean:
                clean = clean.split("```")[1].strip()
            
            start = clean.find('{')
            end = clean.rfind('}')
            if start != -1 and end != -1:
                clean = clean[start:end+1]
            
            import re
            clean = re.sub(r',\s*([\]}])', r'\1', clean)
            return json.loads(clean)
        except Exception as e:
            logger.warning(f"JSON extraction failed: {e}. Raw: {text_content[:100]}...")
            return None

    def _mark_key_limited(self, key, is_dead=False):
        if is_dead:
            self._key_status[key] = {"status": "dead", "retry_after": 0}
        else:
            self._key_status[key] = {"status": "cooled_down", "retry_after": time.time() + 30}

    def _get_client_by_key(self, key):
        if not key: return None, "None"
        is_groq = key.startswith("gsk_")
        if key not in self._clients:
            if is_groq:
                self._clients[key] = AsyncOpenAI(api_key=key, base_url=GROQ_BASE_URL, max_retries=0)
            else:
                self._clients[key] = AsyncOpenAI(api_key=key, max_retries=0)
        
        provider = "Groq" if is_groq else "OpenAI"
        return self._clients[key], provider

    async def translate_text(self, text: str, target_lang: str) -> str:
        """Translate a single piece of text to target_lang with 3-layer failover."""
        if not text or not target_lang or target_lang.lower() == 'english':
            return text
        
        # 1. Prepare candidate pools
        attempt_pools = []
        if self.openai_keys: attempt_pools.append(("openai", self.openai_keys))
        if self.groq_keys: attempt_pools.append(("groq", self.groq_keys))

        for provider, keys in attempt_pools:
            shuffled_keys = list(keys)
            random.shuffle(shuffled_keys)
            
            for i, key in enumerate(shuffled_keys):
                status_info = self._key_status.get(key, {"status": "active"})
                if status_info["status"] == "dead": continue
                if status_info["status"] == "cooled_down" and time.time() < status_info.get("retry_after", 0):
                    continue
                
                try:
                    client, _ = self._get_client_by_key(key)
                    model = "gpt-4o-mini" if provider == "openai" else GROQ_MODEL
                    
                    response = await client.chat.completions.create(
                        model=model,
                        messages=[
                            {
                                "role": "system",
                                "content": f"You are a master news journalist and professional translator. Translate to {target_lang}. RETURN ONLY THE TRANSLATED TEXT."
                            },
                            {"role": "user", "content": text}
                        ],
                        temperature=0.1,
                        timeout=15
                    )
                    return response.choices[0].message.content.strip()
                except Exception as e:
                    error_msg = str(e).lower()
                    is_quota = any(word in error_msg for word in ["quota", "insufficient", "spend", "limit"])
                    if is_quota or "429" in error_msg:
                        self._mark_key_limited(key, is_dead=is_quota)
                    continue

        # 2. FINAL LAYER: NLLB EMERGENCY FALLBACK
        logger.warning(f"LLM layers failed for {target_lang}. Using NLLB fallback.")
        return await self.translate_nllb(text, target_lang)


    async def translate_stories(self, stories: List[Dict[str, Any]], target_lang: str) -> List[Dict[str, Any]]:
        """Translate multiple stories with parallel execution."""
        if not stories or not target_lang or target_lang.lower() == 'english':
            return stories

        translated_stories = json.loads(json.dumps(stories))
        
        async def translate_single_story(story):
            if 'bullets' in story and story['bullets']:
                story['bullets'] = await asyncio.gather(*[self.translate_text(b, target_lang) for b in story['bullets']])
            
            fields_to_translate = ['title', 'summary', 'why', 'affected', 'headline']
            for field in fields_to_translate:
                if field in story and story[field]:
                    story[field] = await self.translate_text(story[field], target_lang)
            return story

        results = []
        batch_size = 3
        for i in range(0, len(translated_stories), batch_size):
            batch = translated_stories[i:i+batch_size]
            results.extend(await asyncio.gather(*[translate_single_story(s) for s in batch]))
            if i + batch_size < len(translated_stories):
                await asyncio.sleep(0.3)

        return results

    async def translate_node_bulk(self, node_data: Dict[str, Any], target_lang: str) -> Dict[str, Any]:
        """Translate entire node dashboard with DB caching and NLLB fallback."""
        if not target_lang or target_lang.lower() == 'english':
            return node_data

        stories = node_data.get("stories", [])
        if not stories:
            return node_data

        def _load_cache_sync():
            db = SessionLocal()
            u_indices = []
            try:
                for idx, story in enumerate(stories):
                    article_id = story.get("id")
                    if article_id and str(article_id).isdigit():
                        article = db.query(VerifiedNews).filter(VerifiedNews.id == int(article_id)).first()
                        if article and article.translation_cache:
                            cache = article.translation_cache
                            if isinstance(cache, str):
                                try: cache = json.loads(cache)
                                except: cache = {}
                            
                            if target_lang in cache:
                                cached_val = cache[target_lang]
                                story.update({
                                    "title": cached_val.get("title", story.get("title")),
                                    "headline": cached_val.get("title", story.get("headline")),
                                    "bullets": cached_val.get("bullets", story.get("bullets")),
                                    "why": cached_val.get("why", story.get("why")),
                                    "affected": cached_val.get("affected", story.get("affected")),
                                    "is_cached": True
                                })
                                continue
                    u_indices.append(idx)
            finally:
                db.close()
            return u_indices
        
        try:
            untranslated_indices = await asyncio.to_thread(_load_cache_sync)
            if not untranslated_indices: return node_data

            to_translate_full = [stories[i] for i in untranslated_indices]
            batch_size = 4
            batches = [to_translate_full[i:i + batch_size] for i in range(0, len(to_translate_full), batch_size)]
            
            async def translate_batch(batch_items, b_idx):
                async with self._concurrency_limit:
                    key, k_idx = self._get_best_key()
                    if not key: return []
                    client, provider = self._get_client_by_key(key)
                    await asyncio.sleep(b_idx * 0.4) 
                
                articles_text = ""
                for idx, story in enumerate(batch_items, 1):
                    bullets = story.get("bullets", [])
                    articles_text += f"ID: {story.get('id', idx)}\nT: {story.get('title')}\nB: {' | '.join(bullets)}\nW: {story.get('why', 'N/A')}\nA: {story.get('affected', 'N/A')}\n---\n"

                max_attempts = 3 # Fast failover for batches
                for attempt in range(max_attempts):
                    try:
                        batch_model = GROQ_MODEL if provider == "Groq" else "gpt-4o-mini"
                        response = await client.chat.completions.create(
                            model=batch_model,
                            messages=[
                                {"role": "system", "content": f"Translate to {target_lang}. Return JSON: {{\"translated\": [ {{ \"id\": \"id\", \"t\": \"title\", \"b\": [\"bullet\"], \"w\": \"why\", \"a\": \"affected\" }} ]}}"},
                                {"role": "user", "content": articles_text}
                            ],
                            temperature=0.1,
                            timeout=30
                        )
                        raw_result = self._clean_json(response.choices[0].message.content.strip())
                        if raw_result and raw_result.get("translated"):
                            return raw_result.get("translated")
                    except Exception as e:
                        key, k_idx = self._get_best_key()
                        if not key: break
                        client, provider = self._get_client_by_key(key)
                
                # BATCH FAILBACK: Single item NLLB
                results = []
                for item in batch_items:
                    results.append({
                        "id": item.get("id"),
                        "t": await self.translate_text(item.get("title"), target_lang),
                        "b": [await self.translate_text(b, target_lang) for b in item.get("bullets", [])],
                        "w": await self.translate_text(item.get("why"), target_lang),
                        "a": await self.translate_text(item.get("affected"), target_lang)
                    })
                return results

            batch_results = await asyncio.gather(*[translate_batch(b, i) for i, b in enumerate(batches)])
            all_translated = [tr for res in batch_results for tr in res]

            def _save_cache_sync():
                db = SessionLocal()
                try:
                    trans_map = {str(tr.get("id")): tr for tr in all_translated}
                    for idx in untranslated_indices:
                        orig = stories[idx]
                        tr = trans_map.get(str(orig.get("id")))
                        if not tr: continue
                        
                        orig.update({
                            "title": tr.get("t"), "headline": tr.get("t"), "bullets": tr.get("b"),
                            "why": tr.get("w"), "affected": tr.get("a"), "is_translated": True
                        })

                        article = db.query(VerifiedNews).filter(VerifiedNews.id == int(orig["id"])).first()
                        if article:
                            cache = article.translation_cache or {}
                            cache[target_lang] = {
                                "title": orig["title"], "bullets": orig["bullets"],
                                "why": orig["why"], "affected": orig["affected"]
                            }
                            article.translation_cache = cache
                            db.commit()
                finally:
                    db.close()

            await asyncio.to_thread(_save_cache_sync)
            return node_data
        except Exception as e:
            logger.error(f"Bulk translation failed: {e}")
            return node_data

    async def _do_translate(self, items: List[Dict[str, str]], target_lang: str, node_title: str = "") -> Dict[str, Any]:
        if not items or not target_lang or target_lang.lower() == 'english':
            return {"translated_stories": items, "node_title": node_title}
        try:
            translated = await self.translate_stories(items, target_lang)
            trans_title = await self.translate_text(node_title, target_lang) if node_title else node_title
            return {"translated_stories": translated, "node_title": trans_title}
        except Exception as e:
            return {"translated_stories": items, "node_title": node_title}
