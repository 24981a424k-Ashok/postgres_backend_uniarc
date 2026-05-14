import re
from typing import Dict, List, Any
from datetime import datetime

class StudentClassifier:
    """
    Lightweight, fast NLP classifier for Student News that avoids heavy ML/LLM models.
    Uses keyword heuristics and regex to categorize, summarize, and extract metadata.
    """
    
    CATEGORIES = {
        "Education Policy & Govt Updates": ["education policy", "syllabus", "ugc", "cbse", "aicte", "nep 2020", "ministry of education", "school board", "educational reform", "government school", "higher education"],
        "Exams & Results": ["exam date", "results declared", "scorecard", "admit card", "nta", "jee main", "neet ug", "upsc prelims", "cut-off marks", "answer key", "date sheet", "counseling", "mock test", "merit list", "board exams", "entrance exam", "examination", "test result", "passing marks"],
        "Scholarships & Internships": ["scholarship", "internship", "fellowship", "stipend", "student grant", "student funding", "financial assistance for students", "startup grant", "startup funding", "training program", "summer internship"],
        "Career & Placement News": ["campus placement", "fresher hiring", "graduate recruitment", "university placement", "off-campus drive", "fresher vacancy", "job for students", "career fair", "recruitment drive", "employment news"],
        "Study Abroad Updates": ["student visa", "study abroad", "ielts", "toefl", "gre", "international student", "foreign university", "overseas education", "visa news", "emigration for study"],
        "AI & Tech for Students": ["student hackathon", "coding competition", "aicte internship", "student bootcamp", "student certification", "campus ambassador", "hackathon", "robotics", "tech fest", "coding challenge"],
        "Education": ["education", "teaching", "schooling", "learning", "classroom", "teacher strike", "pedagogy", "literacy"],
        "Campus Life": ["campus news", "university life", "student union", "college event", "hostel", "campus infrastructure", "student protest"],
        "Admissions & Courses": ["admission open", "apply now", "enrollment", "course curriculum", "degree program", "admission-notice"],
        "Academic Research": ["research paper", "journal", "phd thesis", "scientific discovery", "academic conference", "academic study"]
    }

    PROFILES = {
        "School Student (10th/12th)": ["cbse", "ncert", "class 10", "class 12", "board exam", "icse", "state board", "school student", "secondary education"],
        "Engineering Aspirant": ["jee", "b.tech", "engineering entrance", "gate exam", "iit", "nit", "bitsat", "josaa", "engineering student", "iitian"],
        "Medical Aspirant": ["neet", "mbbs admission", "aiims", "bds", "pharmacy entrance", "ayush counseling", "jipmer", "pgimer", "medical student"],
        "Govt Job Aspirant": ["upsc notification", "ssc cgl", "bank po entrance", "rrb ntpc", "ibps po", "nda exam", "cds notification", "civil services prelims", "sbi po", "state psc", "government job", "employment news"],
        "Graduate/Techie": ["fresher", "placement", "startup", "coding", "software engineer", "developer", "internship", "graduation"]
    }

    AUTHORITIES = ["UGC", "CBSE", "AICTE", "NTA", "UPSC", "SSC", "Ministry of Education", "State Board", "University Grants Commission"]

    URGENCY_KEYWORDS = {
        "High": ["deadline", "tomorrow", "today", "urgent", "breaking", "last date", "closing", "alert"],
        "Medium": ["upcoming", "soon", "next week", "announced", "scheduled", "notification"],
        "Low": ["proposed", "planned", "expected", "future", "report", "study"]
    }
    
    STRICT_KEYWORDS = [
        "university admission", "college admission", "campus placement", "scholarship", 
        "exam result", "admit card", "student visa", "study abroad", "cut-off marks",
        "board exam", "jee main", "neet ug", "education policy", "fellowship", "internship",
        "syllabus", "ncert", "ugc", "cbse", "nta", "upsc notification", "ssc cgl",
        "startup grant", "startup funding", "hackathon", "coding competition",
        "educational", "learning", "academic", "placement drive", "hiring fresher",
        "recruitment drive", "campus hiring", "student achievement", "campus life",
        "exam update", "university news", "school news", "career guide",
        "intern", "stipend", "fresher", "placement", "graduation", "degree", "diploma",
        "research paper", "higher education", "student protest", "tuition fees"
    ]
    
    def _extract_specific_exam(self, text: str) -> str | None:
        # Match standard Indian exams as whole words
        exams = ["NEET", "JEE", "UPSC", "SSC", "CBSE", "CAT", "CLAT", "GATE", "ICSE", "CUET", "NDA", "CDS", "IBPS"]
        for exam in exams:
            if re.search(rf'\b{exam}\b', text, re.IGNORECASE):
                return exam
        return None
    
    def process_article(self, title: str, content: str) -> Dict[str, Any] | None:
        """
        Main pipeline to process a raw article into a structured student news item.
        Returns None if it does not strictly cover Student News keywords.
        """
        combined_text = f"{title} {content}".lower()
        
        # Strict context guard
        strict_matches = 0
        for kw in self.STRICT_KEYWORDS:
            if re.search(rf'\b{kw}\b', combined_text, re.IGNORECASE):
                strict_matches += 1
                
        specific_exam = self._extract_specific_exam(combined_text)
                
        # Relaxation: if the category is Education or it hits at least one strict keyword, it's a match
        if strict_matches == 0 and not specific_exam:
             # Check if it's generally about education
             if "education" in combined_text or "student" in combined_text:
                 # Allow it but maybe with lower trend score
                 pass 
             else:
                 return None
             
        category = self._assign_category(combined_text)
        tags = self._generate_tags(combined_text, category)
        profiles = self._assign_profiles(combined_text)
        direct_links = self._extract_links(text=content)  # Extract links only from content
        
        # Inject exact matching exam if found
        if specific_exam:
            tags.insert(0, f"#{specific_exam}")
            
        dates = self._extract_dates(combined_text)
        authority = self._extract_authority(combined_text)
        urgency = self._determine_urgency(combined_text)
        summary = self._generate_summary(content)
        trend_score = self._calculate_trend_score(combined_text, urgency)
        
        return {
            "title": title,
            "summary": summary,
            "category": category,
            "tags": tags,
            "profiles": profiles,
            "direct_links": direct_links,
            "important_dates": dates,
            "authority": authority,
            "urgency": urgency,
            "trend_score": trend_score
        }

    def _assign_category(self, text: str) -> str:
        best_match = "General Student News"
        max_score = 0
        
        for cat, keywords in self.CATEGORIES.items():
            score = 0
            for kw in keywords:
                if re.search(rf'\b{kw}\b', text, re.IGNORECASE):
                    score += 1
                    
            if score > max_score:
                max_score = score
                best_match = cat
                
        return best_match

    def _generate_tags(self, text: str, category: str) -> List[str]:
        tags = set()
        
        # Base tag on category
        if "Exam" in category: tags.add("#Exam")
        if "Scholarship" in category: tags.add("#Scholarship")
        if "Job" in category or "Career" in category: tags.add("#Job")
        if "Policy" in category: tags.add("#Policy")
        if "Abroad" in category: tags.add("#StudyAbroad")
        if "Tech" in category: tags.add("#Tech")
            
        # Specific keyword tags
        if "nta" in text or "jee" in text or "neet" in text: tags.add("#CompetitiveExams")
        if "cbse" in text or "board" in text: tags.add("#BoardExams")
        if "internship" in text: tags.add("#Internship")
        if "hackathon" in text or "coding" in text: tags.add("#Coding")
            
        return list(tags)[:4] # max 4 tags

    def _extract_dates(self, text: str) -> List[str]:
        # Very lightweight date extraction regex (e.g. 15th Jan, March 20, 2024-05-12)
        dates = []
        # Basic pattern for "DD Month" or "Month DD"
        month_pattern = r'(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*'
        date_pattern = rf'\b(\d{{1,2}}(?:st|nd|rd|th)?\s+{month_pattern}|{month_pattern}\s+\d{{1,2}})\b'
        
        matches = re.findall(date_pattern, text, re.IGNORECASE)
        # matches returns tuples because of groups in regex, clean it up
        for match in re.finditer(date_pattern, text, re.IGNORECASE):
            dates.append(match.group(0).title())
            
        return list(set(dates))[:2] # Top 2 dates extracted

    def _extract_authority(self, text: str) -> str:
        for auth in self.AUTHORITIES:
            if auth.lower() in text:
                return auth
        return "General"

    def _determine_urgency(self, text: str) -> str:
        for level, keywords in self.URGENCY_KEYWORDS.items():
            if any(kw in text for kw in keywords):
                return level
        return "Low"

    def _generate_summary(self, content: str) -> str:
        # Fast 3-line summary approximation by splitting sentences
        # Not perfect NLP, but extremely fast
        if not content:
            return ""
            
        # Clean text slightly
        text = re.sub(r'\s+', ' ', content).strip()
        
        # Split by periods roughly
        sentences = [s.strip() + "." for s in text.split('.') if len(s.strip()) > 15]
        
        if len(sentences) <= 3:
            return " ".join(sentences)
            
        # Take first 2 sentences and try to find one with numbers/dates for the 3rd
        summary_sents = sentences[:2]
        
        best_third = sentences[2]
        for s in sentences[2:6]:
            if re.search(r'\d', s):
                best_third = s
                break
                
        summary_sents.append(best_third)
        return " ".join(summary_sents)

    def _calculate_trend_score(self, text: str, urgency: str) -> int:
        score = 10
        if urgency == "High":
            score += 40
        elif urgency == "Medium":
            score += 20
            
        # Boost score based on strong keywords
        hot_keywords = ["deadline", "released", "declared", "announce", "breaking", "major"]
        score += sum(15 for kw in hot_keywords if kw in text)
        
        return min(score, 100)

    def _assign_profiles(self, text: str) -> List[str]:
        profiles = []
        for profile, keywords in self.PROFILES.items():
            if any(kw in text for kw in keywords):
                profiles.append(profile)
        # default to all if none matched
        if not profiles:
            profiles.append("General Student")
        return profiles

    def _extract_links(self, text: str) -> List[str]:
        # Extract http/https links
        urls = re.findall(r'https?://[^\s<>"]+|www\.[^\s<>"]+', text)
        return list(set(urls))[:2]
