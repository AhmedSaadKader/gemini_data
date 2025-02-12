import re
from typing import Dict, List, Tuple
from collections import defaultdict
import csv
from datetime import datetime
import os
import sys
from pathlib import Path
import logging
logging.basicConfig(level=logging.DEBUG)

class SuggestionAnalyzer:
    def __init__(self, filename: str):
        self.filename = filename
        self.standard_changes = []
        self.similar_pairs = []
        self.categories = defaultdict(list)
        
    def parse_file(self):
      """Parse the suggestions file and categorize changes."""
      if not os.path.exists(self.filename):
          raise FileNotFoundError(f"Could not find suggestions file: {self.filename}")
          
      print(f"Parsing file: {self.filename}")
      current_section = None
      
      try:
          with open(self.filename, 'r', encoding='utf-8') as f:
              lines = f.readlines()
              
          print(f"Found {len(lines)} lines in file")
          
          # Initialize variables for multi-line statement handling
          current_statement = []
          in_statement = False
          
          for line_num, line in enumerate(lines, 1):
              line = line.strip()
              
              # Skip empty lines
              if not line:
                  continue
                  
              # Check for section headers
              if line.startswith('--'):
                  if 'Standard name changes' in line:
                      current_section = 'standard'
                      print("Found standard name changes section")
                  elif 'Similar ingredients to review' in line:
                      current_section = 'similar'
                      print("Found similar ingredients section")
                  continue
              
              # Handle multi-line SQL statements for standard changes
              if current_section == 'standard':
                  if line.startswith('UPDATE'):
                      in_statement = True
                      current_statement = [line]
                  elif in_statement:
                      current_statement.append(line)
                      if line.endswith(';'):
                          # Process complete statement
                          full_statement = ' '.join(current_statement)
                          
                          # Extract ingredient names from complete statement
                          match = re.search(r"SET ingredient_name = '([^']+)'\s*WHERE ingredient_name = '([^']+)'", full_statement)
                          if match:
                              new_name, old_name = match.groups()
                              self.standard_changes.append((old_name, new_name))
                              self._categorize_change(old_name, new_name)
                          
                          in_statement = False
                          current_statement = []
              
              # Handle similar pairs
              elif current_section == 'similar':
                  if 'Possible duplicate' in line:
                      # Extract similarity
                      sim_match = re.search(r'\((0\.\d+) similarity\)', line)
                      if sim_match:
                          similarity = float(sim_match.group(1))
                          
                          # Look ahead for Keep and Delete lines
                          keep_name = None
                          delete_name = None
                          
                          # Look at next few lines
                          for j in range(1, 5):
                              if line_num + j >= len(lines):
                                  break
                              next_line = lines[line_num + j].strip()
                              
                              if '-- Keep:' in next_line:
                                  keep_match = re.search(r"-- Keep: '([^']+)'", next_line)
                                  if keep_match:
                                      keep_name = keep_match.group(1)
                              elif '-- Delete:' in next_line:
                                  delete_match = re.search(r"-- Delete: '([^']+)'", next_line)
                                  if delete_match:
                                      delete_name = delete_match.group(1)
                          
                          if keep_name and delete_name:
                              self.similar_pairs.append((keep_name, delete_name, similarity))
                              print(f"Found similar pair: {keep_name} -> {delete_name} ({similarity})")
          
          print(f"Processed {len(self.standard_changes)} standard changes")
          print(f"Processed {len(self.similar_pairs)} similar pairs")
            
      except Exception as e:
          print(f"Error parsing file at line {line_num}: {str(e)}")
          raise 

    def _categorize_change(self, old_name: str, new_name: str):
        """Categorize the type of change."""
        if old_name.lower() == new_name.lower():
            self.categories['case_changes'].append((old_name, new_name))
        elif ' extract' in old_name.lower() and ' extract' not in new_name.lower():
            self.categories['remove_extract'].append((old_name, new_name))
        elif old_name.count(' ') != new_name.count(' '):
            self.categories['word_changes'].append((old_name, new_name))
        elif any(c.isdigit() for c in old_name) and not any(c.isdigit() for c in new_name):
            self.categories['remove_numbers'].append((old_name, new_name))
        else:
            self.categories['other'].append((old_name, new_name))
    
    def generate_review_files(self, output_dir: str = 'review_files'):
        """Generate separate CSV files for different types of changes."""
        try:
            # Create output directory if it doesn't exist
            os.makedirs(output_dir, exist_ok=True)
            print(f"Saving review files to: {os.path.abspath(output_dir)}")
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M")
            files_generated = []
            
            # Generate standard changes report
            for category, changes in self.categories.items():
                if changes:
                    filename = os.path.join(output_dir, f'review_{category}_{timestamp}.csv')
                    with open(filename, 'w', newline='', encoding='utf-8') as f:
                        writer = csv.writer(f)
                        writer.writerow(['Old Name', 'New Name', 'Approve (Y/N)', 'Notes'])
                        writer.writerows([(old, new, '', '') for old, new in changes])
                    files_generated.append(filename)
                    print(f"Generated {filename} with {len(changes)} entries")
            
            # Generate similar pairs report
            if self.similar_pairs:
                filename = os.path.join(output_dir, f'review_similar_pairs_{timestamp}.csv')
                with open(filename, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    writer.writerow(['Keep Name', 'Delete Name', 'Similarity', 'Approve (Y/N)', 'Notes'])
                    # Sort by similarity score in descending order
                    sorted_pairs = sorted(self.similar_pairs, key=lambda x: x[2], reverse=True)
                    writer.writerows([(keep, delete, f"{sim:.3f}", '', '') for keep, delete, sim in sorted_pairs])
                files_generated.append(filename)
                print(f"Generated {filename} with {len(self.similar_pairs)} entries")
            
            return files_generated
            
        except Exception as e:
            print(f"Error generating review files: {str(e)}")
            raise
    
    def generate_summary(self) -> str:
        """Generate a summary of the changes."""
        summary = ["\n=== Suggestion Analysis Summary ===\n"]
        
        if self.categories:
            summary.append("Standard Name Changes:")
            for category, changes in sorted(self.categories.items()):
                summary.append(f"- {category.replace('_', ' ').title()}: {len(changes)} changes")
        else:
            summary.append("No standard name changes found")
        
        if self.similar_pairs:
            summary.append(f"\nSimilar Pairs to Review: {len(self.similar_pairs)} pairs")
            # Group by similarity ranges
            ranges = {
                "Very High (>0.95)": 0,
                "High (0.90-0.95)": 0,
                "Medium (0.85-0.90)": 0,
                "Low (<0.85)": 0
            }
            for _, _, sim in self.similar_pairs:
                if sim > 0.95:
                    ranges["Very High (>0.95)"] += 1
                elif sim > 0.90:
                    ranges["High (0.90-0.95)"] += 1
                elif sim > 0.85:
                    ranges["Medium (0.85-0.90)"] += 1
                else:
                    ranges["Low (<0.85)"] += 1
            
            summary.append("Similarity breakdown:")
            for range_name, count in ranges.items():
                if count > 0:
                    summary.append(f"  - {range_name}: {count} pairs")
        else:
            summary.append("\nNo similar pairs found for review")
        
        return '\n'.join(summary)

def find_suggestion_file(directory: str = '.') -> str:
    """Find the most recent suggestions file in the directory."""
    suggestion_files = []
    for file in os.listdir(directory):
        if file.startswith('suggested_changes_') and file.endswith('.sql'):
            full_path = os.path.join(directory, file)
            suggestion_files.append((full_path, os.path.getmtime(full_path)))
    
    if not suggestion_files:
        raise FileNotFoundError("No suggestion files found. Looking for files named 'suggested_changes_*.sql'")
    
    # Return the most recent file
    return max(suggestion_files, key=lambda x: x[1])[0]

def main():
    try:
        # Try to find the suggestions file
        try:
            suggestions_file = find_suggestion_file()
            print(f"Found suggestions file: {suggestions_file}")
        except FileNotFoundError as e:
            print(f"Error: {e}")
            print("Please ensure the suggestions file is in the current directory")
            return
        
        # Initialize and run the analyzer
        analyzer = SuggestionAnalyzer(suggestions_file)
        analyzer.parse_file()
        
        # Generate review files
        output_dir = 'suggestion_reviews'
        generated_files = analyzer.generate_review_files(output_dir)
        
        # Print summary
        print(analyzer.generate_summary())
        
        if generated_files:
            print("\nGenerated review files:")
            for file in generated_files:
                print(f"- {file}")
            
            print("\nNext steps:")
            print("1. Review each CSV file")
            print("2. Mark 'Y' or 'N' in the 'Approve' column")
            print("3. Add any notes in the 'Notes' column")
            print("4. Run the SQL generator script (to be created) with your reviewed CSVs")
        
    except Exception as e:
        print(f"\nError: {str(e)}")
        print("\nFor troubleshooting:")
        print("1. Ensure the suggestions file exists and is readable")
        print("2. Check that the file contains the expected sections")
        print("3. Verify the file encoding is UTF-8")

if __name__ == "__main__":
    main()