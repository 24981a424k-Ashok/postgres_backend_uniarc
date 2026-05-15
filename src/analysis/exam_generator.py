import json
import logging
import random
import os
from collections import defaultdict
from datetime import datetime, timedelta
from typing import List, Dict
from sqlalchemy.orm import Session
from src.database.models import VerifiedNews, DailyDigest
from src.analysis.llm_analyzer import LLMAnalyzer

logger = logging.getLogger(__name__)

class ExamGenerator:
    def __init__(self):
        self.llm = LLMAnalyzer()

    def get_recent_news(self, db: Session) -> List[Dict]:
        """Fetch verified news, falling back to 7 days if last 24h is empty."""
        now = datetime.utcnow()
        last_24h = now - timedelta(hours=24)
        
        # Try last 24h first for maximum freshness
        news = db.query(VerifiedNews).filter(
            VerifiedNews.created_at >= last_24h,
            VerifiedNews.impact_score >= 4
        ).order_by(VerifiedNews.impact_score.desc()).limit(50).all()
        
        if not news:
            logger.info("No news in last 24h, falling back to 7 days.")
            last_7d = now - timedelta(days=7)
            news = db.query(VerifiedNews).filter(
                VerifiedNews.created_at >= last_7d,
                VerifiedNews.impact_score >= 3  # Lower score threshold for fallback
            ).order_by(VerifiedNews.impact_score.desc()).limit(50).all()

        return [n.to_dict() for n in news]


    async def generate_mock_test(self, db: Session) -> Dict:
        return await self.generate_from_news(db)

    async def generate_from_news(self, db: Session) -> Dict:
        """Alias for generate_mock_test to fix attribute error."""
        news_items = self.get_recent_news(db)
        
        if not news_items:
            # Fallback ONLY to latest digest if it was published in the last 24h
            latest = db.query(DailyDigest).order_by(DailyDigest.date.desc()).first()
            if latest and latest.date >= (datetime.utcnow() - timedelta(hours=24)):
                news_items = latest.content_json.get('top_stories', [])

        if not news_items:
            return {"status": "error", "message": "Intelligence scan found no fresh news (last 24h). Please run a news cycle first."}

        # Perfection: Ensure diverse category representation
        categorized_news = defaultdict(list)
        for n in news_items:
            cat = (n.get('category') or 'General').strip().capitalize()
            categorized_news[cat].append(n)
        
        # Select balanced items (Aim for 4 items per major category)
        balanced_items = []
        major_cats = ['National', 'International', 'Economy', 'Science', 'Sports']
        for cat in major_cats:
            items = categorized_news.get(cat, [])
            random.shuffle(items)
            balanced_items.extend(items[:4])
            
        remaining = [n for n in news_items if n not in balanced_items]
        random.shuffle(remaining)
        balanced_items.extend(remaining)
        
        # Use balanced selection for text context
        news_text = ""
        for n in balanced_items[:25]: 
            title = n.get('title', 'Unknown')
            why = n.get('why_it_matters') or n.get('why', '')
            bullets = ", ".join(n.get('summary_bullets') or n.get('bullets', []))
            cat = n.get('category', 'General')
            news_text += f"ARTICLE: {title}\nFACTS: {bullets}\nIMPACT: {why}\nCATEGORY: {cat}\n\n"

        logging.info(f"Generating balanced questions from {len(balanced_items)} nodes across {len(categorized_news)} categories.")

        
        prompt = f"""
        You are an AI Current Affairs Exam Expert specializing in Indian and global competitive exams (UPSC, SSC, Banking).
        
        Create a Daily Current Affairs Mock Test based on the following news:
        {news_text}
        
        RULES:
        1. Generate exactly 15 questions.
        2. Format: 
           - 15 MCQs (4 options)
        3. Sections: Mix of National, International, Economy, Science, or Sports.
        4. Output JSON format ONLY:
        {{
            "title": "Daily Mock Test - {datetime.now().strftime('%Y-%m-%d')}",
            "questions": [
                {{
                    "id": 1,
                    "type": "MCQ",
                    "section": "National Affairs",
                    "question": "...",
                    "options": ["A", "B", "C", "D"],
                    "correct_answer": "A",
                    "explanation": "..."
                }}
            ]
        }}
        """
        
        try:
            # Perfection: Use the async get_completion method correctly
            response = await self.llm.get_completion(
                system_prompt="You are an AI Current Affairs Exam Expert. Output ONLY valid JSON.",
                user_prompt=prompt
            )
            # Robust JSON cleanup
            content = response.strip()
            if "{" in content and "}" in content:
                content = content[content.find("{"):content.rfind("}")+1]
            return json.loads(content)
        except Exception as e:
            logging.error(f"Exam Generation Error: {e}")
            
            # Fallback: Load from question bank
            try:
                from src.config.settings import DATA_DIR
                bank_path = DATA_DIR / 'question_bank.json'
                
                if os.path.exists(bank_path):
                    with open(bank_path, 'r', encoding='utf-8') as f:
                        all_questions = json.load(f)
                    
                    # Randomly select up to 3 questions
                    count = min(len(all_questions), 15)
                    selected_questions = random.sample(all_questions, count)
                    
                    # Re-index ids
                    for idx, q in enumerate(selected_questions):
                        q['id'] = idx + 1
                        
                    return {
                        "title": f"Daily Mock Test - Smart Fallback - {datetime.now().strftime('%d %b %Y')}",
                        "questions": selected_questions
                    }
                else:
                    logging.warning(f"Question bank not found at {bank_path}. Using hardcoded fallback.")
                    raise FileNotFoundError("Bank missing")

            except Exception as bank_error:
                logging.error(f"Fallback Bank Error: {bank_error}")
                
                # Enhanced Ultimate Fallback List (minimum 5 varied questions)
                fallback_questions = [
                    {"id": 1, "type": "MCQ", "section": "General", "question": "Which organization releases the 'World Economic Outlook'?", "options": ["IMF", "World Bank", "WEF", "ADB"], "correct_answer": "IMF", "explanation": "The IMF releases the WEO report."},
                    {"id": 2, "type": "MCQ", "section": "Sports", "question": "Who won the Men's ODI World Cup 2023?", "options": ["India", "Australia", "England", "New Zealand"], "correct_answer": "Australia", "explanation": "Australia defeated India in the final."},
                    {"id": 3, "type": "MCQ", "section": "Science", "question": "Which mission recently landed on the south pole of the Moon?", "options": ["Chandrayaan-2", "Chandrayaan-3", "Luna 25", "Artemis I"], "correct_answer": "Chandrayaan-3", "explanation": "India's Chandrayaan-3 was the first to land on the lunar south pole."},
                    {"id": 4, "type": "MCQ", "section": "Economy", "question": "What is the current Repo Rate as per the latest RBI MPC meeting?", "options": ["6.25%", "6.50%", "6.75%", "6.00%"], "correct_answer": "6.50%", "explanation": "RBI has maintained the repo rate at 6.50% in recent cycles."},
                    {"id": 5, "type": "MCQ", "section": "National", "question": "Who is the current President of India?", "options": ["Ram Nath Kovind", "Droupadi Murmu", "Jagdeep Dhankhar", "Yashwant Sinha"], "correct_answer": "Droupadi Murmu", "explanation": "Smt. Droupadi Murmu is the 15th President of India."},
                    {"id": 6, "type": "MCQ", "section": "International", "question": "Which country recently joined NATO as its 31st member?", "options": ["Sweden", "Finland", "Ukraine", "Japan"], "correct_answer": "Finland", "explanation": "Finland officially joined NATO in April 2023."},
                    {"id": 7, "type": "MCQ", "section": "Science", "question": "What is the primary objective of the Aditya-L1 mission?", "options": ["Lunar exploration", "Mars orbit", "Solar observation", "Venus atmosphere"], "correct_answer": "Solar observation", "explanation": "Aditya-L1 is India's first solar observatory mission."},
                    {"id": 8, "type": "MCQ", "section": "Economy", "question": "Which Indian state has the highest GST collection currently?", "options": ["Gujarat", "Maharashtra", "Karnataka", "Tamil Nadu"], "correct_answer": "Maharashtra", "explanation": "Maharashtra consistently leads in GST collections in India."},
                    {"id": 9, "type": "MCQ", "section": "Sports", "question": "Who holds the record for most centuries in ODI Cricket?", "options": ["Sachin Tendulkar", "Virat Kohli", "Ricky Ponting", "Rohit Sharma"], "correct_answer": "Virat Kohli", "explanation": "Virat Kohli surpassed Sachin Tendulkar's record in 2023."},
                    {"id": 10, "type": "MCQ", "section": "Science", "question": "Which AI model was developed by Google DeepMind to predict protein structures?", "options": ["AlphaGo", "AlphaFold", "Gemini", "PaLM"], "correct_answer": "AlphaFold", "explanation": "AlphaFold has revolutionized biological research by predicting protein structures."},
                    {"id": 11, "type": "MCQ", "section": "National", "question": "What is the name of India's indigenous payment gateway?", "options": ["Visa", "RuPay", "Mastercard", "Amex"], "correct_answer": "RuPay", "explanation": "RuPay is India's indigenous card payment network."},
                    {"id": 12, "type": "MCQ", "section": "International", "question": "Where is the headquarters of the United Nations located?", "options": ["Geneva", "Paris", "New York", "Vienna"], "correct_answer": "New York", "explanation": "The UN headquarters is in New York City."},
                    {"id": 13, "type": "MCQ", "section": "Economy", "question": "What does 'FDI' stand for in business?", "options": ["Fixed Deposit Investment", "Foreign Direct Investment", "Financial Data Integration", "First Direct Interest"], "correct_answer": "Foreign Direct Investment", "explanation": "FDI refers to investment made by a firm or individual in one country into business interests located in another country."},
                    {"id": 14, "type": "MCQ", "section": "National", "question": "Which city is known as the 'Silicon Valley of India'?", "options": ["Hyderabad", "Pune", "Bengaluru", "Chennai"], "correct_answer": "Bengaluru", "explanation": "Bengaluru is the hub of India's information technology industry."},
                    {"id": 15, "type": "MCQ", "section": "Science", "question": "What is the main gas found in the atmosphere of Venus?", "options": ["Oxygen", "Nitrogen", "Carbon Dioxide", "Hydrogen"], "correct_answer": "Carbon Dioxide", "explanation": "Venus has a thick atmosphere primarily composed of CO2."},
                    {"id": 16, "type": "MCQ", "section": "Sports", "question": "In which city were the first Asian Games held?", "options": ["New Delhi", "Tokyo", "Bangkok", "Seoul"], "correct_answer": "New Delhi", "explanation": "The first Asian Games were held in New Delhi in 1951."},
                    {"id": 17, "type": "MCQ", "section": "International", "question": "Which organization is responsible for global health monitoring?", "options": ["UNICEF", "WHO", "UNESCO", "WTO"], "correct_answer": "WHO", "explanation": "The World Health Organization (WHO) leads global health initiatives."},
                    {"id": 18, "type": "MCQ", "section": "Science", "question": "Which element has the atomic number 1?", "options": ["Helium", "Hydrogen", "Carbon", "Oxygen"], "correct_answer": "Hydrogen", "explanation": "Hydrogen is the first element in the periodic table."},
                    {"id": 19, "type": "MCQ", "section": "Economy", "question": "Who is the current Governor of the Reserve Bank of India?", "options": ["Urjit Patel", "Shaktikanta Das", "Raghuram Rajan", "Nirmala Sitharaman"], "correct_answer": "Shaktikanta Das", "explanation": "Shaktikanta Das is the 25th Governor of RBI."},
                    {"id": 20, "type": "MCQ", "section": "National", "question": "Which article of the Indian Constitution deals with 'Equality before Law'?", "options": ["Article 14", "Article 17", "Article 21", "Article 32"], "correct_answer": "Article 14", "explanation": "Article 14 guarantees equality before the law."},
                    {"id": 21, "type": "MCQ", "section": "Sports", "question": "Which team won the IPL 2024?", "options": ["CSK", "KKR", "SRH", "RCB"], "correct_answer": "KKR", "explanation": "Kolkata Knight Riders won the IPL 2024 title."},
                    {"id": 22, "type": "MCQ", "section": "International", "question": "Which country is the largest producer of crude oil in the world?", "options": ["Saudi Arabia", "USA", "Russia", "Iraq"], "correct_answer": "USA", "explanation": "The USA has become the world's top producer of crude oil."},
                    {"id": 23, "type": "MCQ", "section": "Science", "question": "What is the speed of light?", "options": ["300,000 km/s", "150,000 km/s", "450,000 km/s", "600,000 km/s"], "correct_answer": "300,000 km/s", "explanation": "Light travels at approximately 3 lakh km per second."},
                    {"id": 24, "type": "MCQ", "section": "National", "question": "Which river is known as the 'Ganges of the South'?", "options": ["Godavari", "Cauvery", "Krishna", "Narmada"], "correct_answer": "Godavari", "explanation": "Godavari is often referred to as Dakshina Ganga."},
                    {"id": 25, "type": "MCQ", "section": "General", "question": "What is the capital of Japan?", "options": ["Kyoto", "Osaka", "Tokyo", "Nagoya"], "correct_answer": "Tokyo", "explanation": "Tokyo is the current capital and most populous city of Japan."}
                ]
                
                return {
                    "status": "success",
                    "title": f"Daily Mock Test (Smart Fallback) - {datetime.now().strftime('%d %b %Y')}",
                    "questions": random.sample(fallback_questions, 15)
                }

