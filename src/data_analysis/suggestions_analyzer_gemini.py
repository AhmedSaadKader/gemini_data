import os
import json
import time
import re
from datetime import datetime
import logging
from typing import List, Dict, Tuple, Optional
from .. import gemini_api

class SuggestionAnalyzer:
    def __init__(self, file_path: str, batch_size: int = 10, max_daily_requests: int = 1400):
        self.file_path = file_path
        self.batch_size = batch_size
        self.max_daily_requests = max_daily_requests
        self.requests_made = 0
        self.last_request_time = 0
        self.setup_logging()
        gemini_api.initialize_gemini()

    def setup_logging(self):
        log_filename = f'suggestion_analysis_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
        logging.basicConfig(
            filename=log_filename,
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        logging.getLogger().addHandler(console_handler)

    def parse_sql_statement(self, line: str) -> Optional[Dict]:
        """Parse SQL UPDATE statement to extract old and new values."""
        try:
            # Match the UPDATE statement pattern
            update_match = re.search(
                r"UPDATE\s+active_ingredients_extended\s+SET\s+ingredient_name\s*=\s*'([^']+)'\s*"
                r"WHERE\s+ingredient_name\s*=\s*'([^']+)'",
                line,
                re.IGNORECASE
            )
            
            if update_match:
                new_name, old_name = update_match.groups()
                return {
                    'type': 'update',
                    'statement': line,
                    'old_name': old_name,
                    'new_name': new_name
                }
            
            # Match possible duplicate pattern
            duplicate_match = re.search(
                r"--\s*Keep:\s*'([^']+)'\s*--\s*Delete:\s*'([^']+)'",
                line
            )
            
            if duplicate_match:
                keep_name, delete_name = duplicate_match.groups()
                return {
                    'type': 'duplicate',
                    'statement': line,
                    'keep_name': keep_name,
                    'delete_name': delete_name
                }
                
            return None
            
        except Exception as e:
            logging.error(f"Error parsing SQL statement: {e}")
            return None

    def read_suggestions(self) -> List[Dict]:
        """Read and parse the suggestions file."""
        suggestions = []
        
        try:
            with open(self.file_path, 'r', encoding='utf-8') as f:
                content = f.read()
                
            # Split into individual statements
            statements = re.split(r';|\n\n', content)
            
            for statement in statements:
                statement = statement.strip()
                if not statement or statement.startswith('--'):
                    continue
                
                parsed = self.parse_sql_statement(statement)
                if parsed:
                    suggestions.append(parsed)
            
            logging.info(f"Successfully parsed {len(suggestions)} suggestions")
            return suggestions
            
        except Exception as e:
            logging.error(f"Error reading suggestions file: {e}")
            raise

    def create_analysis_prompt(self, batch: List[Dict]) -> str:
        """Create a prompt for analyzing a batch of suggestions."""
        suggestions_text = []
        for idx, sugg in enumerate(batch, 1):
            if sugg['type'] == 'update':
                suggestions_text.append(f"Suggestion {idx}:\n"
                                     f"Change '{sugg['old_name']}' to '{sugg['new_name']}'")
            elif sugg['type'] == 'duplicate':
                suggestions_text.append(f"Suggestion {idx}:\n"
                                     f"Keep '{sugg['keep_name']}' and remove '{sugg['delete_name']}'")

        return f"""You are a pharmaceutical database expert. Analyze these drug ingredient name changes:

{chr(10).join(suggestions_text)}

For each suggestion, determine:
1. Is the change linguistically correct? Consider spelling, capitalization, and medical terminology.
2. Does it maintain semantic meaning? The change should not alter the fundamental meaning of the ingredient.
3. Are there any risks? Consider potential confusion with other ingredients.
4. Should this change be approved? Provide a clear yes/no recommendation.

Respond with a JSON array where each object has these exact fields:
- suggestion_number: integer
- original: string (original name)
- proposed: string (new name)
- linguistically_correct: boolean
- maintains_semantics: boolean
- risks: string (description of any risks)
- recommendation: "approve" or "reject"
- notes: string (explanation of decision)"""

    def analyze_batch(self, batch: List[Dict]) -> List[Dict]:
        """Analyze a batch of suggestions using Gemini API."""
        if not batch:
            return []
            
        batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        logging.info(f"\nProcessing batch {batch_id}")
        
        # Log input suggestions
        for idx, suggestion in enumerate(batch, 1):
            logging.info(f"Suggestion {idx}:")
            for key, value in suggestion.items():
                logging.info(f"  {key}: {value}")

        # Create and log prompt
        prompt = self.create_analysis_prompt(batch)
        logging.info(f"\nPrompt for batch {batch_id}:")
        logging.info(prompt)

        # Respect rate limits
        self.wait_for_rate_limit()

        try:
            # Make API call
            response, _ = gemini_api.generate_content(prompt)
            self.requests_made += 1

            if not response:
                logging.error("Empty response from Gemini API")
                return []

            # Log raw response
            logging.info(f"\nRaw Gemini response for batch {batch_id}:")
            logging.info(response)

            # Parse and validate response
            try:
                analysis_results = json.loads(response)
                if not isinstance(analysis_results, list):
                    logging.error("Invalid response format - expected JSON array")
                    return []
                    
                # Log parsed results
                logging.info("\nParsed analysis results:")
                for result in analysis_results:
                    logging.info(json.dumps(result, indent=2))
                
                return analysis_results
                
            except json.JSONDecodeError as e:
                logging.error(f"Failed to parse Gemini response as JSON: {e}")
                return []

        except Exception as e:
            logging.error(f"Error analyzing batch {batch_id}: {str(e)}")
            return []

    def wait_for_rate_limit(self):
        """Implement rate limiting."""
        current_time = time.time()
        
        # Check daily limit
        if self.requests_made >= self.max_daily_requests:
            raise Exception("Daily request limit reached")
        
        # Check RPM limit (15 requests per minute)
        if current_time - self.last_request_time < 4:  # ~15 requests per minute
            sleep_time = 4 - (current_time - self.last_request_time)
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()

    def save_results(self, results: List[Dict], is_final: bool = False):
        """Save analysis results to file with detailed logging."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Handle empty results
        if not results:
            logging.info("No results to save")
            return
            
        # Save detailed log
        log_filename = f'analysis_detailed_log_{timestamp}.txt'
        with open(log_filename, 'w', encoding='utf-8') as f:
            f.write(f"Analysis {'Final ' if is_final else ''}Report - {datetime.now()}\n")
            f.write("=" * 80 + "\n\n")
            
            for result in results:
                f.write(f"Suggestion {result.get('suggestion_number', 'N/A')}:\n")
                f.write("-" * 40 + "\n")
                f.write(f"Original: {result.get('original', 'N/A')}\n")
                f.write(f"Proposed: {result.get('proposed', 'N/A')}\n")
                f.write(f"Analysis:\n")
                f.write(f"  - Linguistically correct: {result.get('linguistically_correct', 'N/A')}\n")
                f.write(f"  - Maintains semantics: {result.get('maintains_semantics', 'N/A')}\n")
                f.write(f"  - Risks: {result.get('risks', 'None')}\n")
                f.write(f"  - Recommendation: {result.get('recommendation', 'N/A')}\n")
                if result.get('notes'):
                    f.write(f"  - Notes: {result['notes']}\n")
                f.write("\n")
        
        # Save JSON results
        json_filename = f'analysis_results{"_final" if is_final else ""}_{timestamp}.json'
        with open(json_filename, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2)
        
        # Generate summary statistics
        total_suggestions = len(results)
        approved = sum(1 for r in results if r.get('recommendation') == 'approve')
        rejected = sum(1 for r in results if r.get('recommendation') == 'reject')
        needs_review = total_suggestions - approved - rejected
        
        logging.info(f"\nResults saved:")
        logging.info(f"  - Detailed log: {log_filename}")
        logging.info(f"  - JSON data: {json_filename}")
        logging.info("\nSummary statistics:")
        logging.info(f"  - Total suggestions analyzed: {total_suggestions}")
        if total_suggestions > 0:
            logging.info(f"  - Approved: {approved} ({(approved/total_suggestions)*100:.1f}%)")
            logging.info(f"  - Rejected: {rejected} ({(rejected/total_suggestions)*100:.1f}%)")
            logging.info(f"  - Needs review: {needs_review} ({(needs_review/total_suggestions)*100:.1f}%)")

    def run_analysis(self):
        """Main analysis function."""
        try:
            suggestions = self.read_suggestions()
            if not suggestions:
                logging.error("No valid suggestions found to analyze")
                return

            total_batches = (len(suggestions) + self.batch_size - 1) // self.batch_size
            logging.info(f"Starting analysis of {len(suggestions)} suggestions in {total_batches} batches")
            
            results = []
            for i in range(0, len(suggestions), self.batch_size):
                batch = suggestions[i:i + self.batch_size]
                batch_num = i // self.batch_size + 1
                
                logging.info(f"Processing batch {batch_num}/{total_batches}")
                
                try:
                    batch_results = self.analyze_batch(batch)
                    if batch_results:
                        results.extend(batch_results)
                        # Save intermediate results
                        self.save_results(results, is_final=False)
                    
                except Exception as e:
                    logging.error(f"Error processing batch {batch_num}: {e}")
                    # Save progress before failing
                    if results:
                        self.save_results(results, is_final=True)
                    raise
            
            # Save final results
            if results:
                self.save_results(results, is_final=True)
            
        except Exception as e:
            logging.error(f"Analysis failed: {e}")
            raise

def main():
    analyzer = SuggestionAnalyzer(
        file_path='suggested_changes.sql',
        batch_size=50,  # Process 10 suggestions per batch
        max_daily_requests=1400  # Leave some buffer for daily limit
    )
    analyzer.run_analysis()

if __name__ == "__main__":
    main()