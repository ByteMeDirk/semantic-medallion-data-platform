import csv
import os
import random
from datetime import datetime

# Lists of entities based on the known entities files
persons = [
    "Bill Gates",
    "Steve Jobs",
    "Elon Musk",
    "Mark Zuckerberg",
    "Sundar Pichai",
    "Tim Cook",
    "Satya Nadella",
    "Michael Dell",
    "Larry Page",
    "Sergey Brin",
    "Jeff Bezos",
    "Jack Dorsey",
    "Susan Wojcicki",
    "Sheryl Sandberg",
    "Reed Hastings",
    "Tim Berners-Lee",
    "Ada Lovelace",
    "Marissa Mayer",
    "Sam Altman",
    "Jensen Huang",
    "Lisa Su",
    "Andy Jassy",
    "Safra Catz",
    "Shantanu Narayen",
    "Arvind Krishna",
]

organizations = [
    "Microsoft",
    "Apple",
    "Tesla",
    "SpaceX",
    "Meta",
    "Facebook",
    "Google",
    "Alphabet",
    "Amazon",
    "Dell Technologies",
    "Twitter",
    "Netflix",
    "Oracle",
    "Adobe",
    "IBM",
    "Intel",
    "AMD",
    "Nvidia",
    "OpenAI",
    "Salesforce",
    "Uber",
    "Lyft",
    "AirBnB",
    "Stripe",
    "LinkedIn",
    "Pinterest",
    "Samsung",
]

locations = [
    "Medina",
    "Seattle",
    "Los Altos",
    "Cupertino",
    "Palo Alto",
    "Menlo Park",
    "Mountain View",
    "Redmond",
    "Austin",
    "Round Rock",
    "San Francisco",
    "New York",
    "Chicago",
    "Denver",
    "Boston",
    "Los Angeles",
    "Portland",
    "Washington DC",
    "Atlanta",
    "Miami",
    "Toronto",
    "Vancouver",
    "London",
    "Berlin",
    "Tokyo",
]

# Templates for generating sentences with named entities
templates = [
    "During the {loc} meeting, {person1} of {org1} discussed strategies with {person2} from {org2}.",
    "At the {loc} conference, {person1} and {person2} represented {org1} and {org2} respectively.",
    "{person1} met with {person2} to discuss {org1}'s expansion into {loc} markets.",
    "{person1} and {person2} are pioneering new technologies at {org1} and {org2} in {loc}.",
    "The partnership between {org1} and {org2}, led by {person1} and {person2}, focuses on {loc} markets.",
    "{person1}, CEO of {org1}, announced a new initiative in {loc} alongside {person2} from {org2}.",
    "The collaboration between {person1} of {org1} and {person2} of {org2} will transform operations in {loc}.",
    "During their {loc} summit, {person1} and {person2} finalized the merger between {org1} and {org2}.",
    "The {org1} headquarters in {loc} hosted {person1} and {person2} for the annual {org2} partnership review.",
    "{person1} from {org1} presented the keynote at the {loc} tech summit, with {person2} of {org2} as respondent.",
    "The {loc}-based research team led by {person1} of {org1} collaborated with {person2} from {org2} on AI ethics.",
    "{person1} and {person2} met at the {org1} campus in {loc} to discuss potential synergies with {org2}.",
    "After the {loc} acquisition, {person1} of {org1} appointed {person2} from {org2} as the new CTO.",
    "The joint venture between {org1} and {org2} was announced by {person1} and {person2} at their {loc} press conference.",
    "{org1} and {org2} held a hackathon in {loc}, judged by industry veterans {person1} and {person2}.",
]


def generate_ner_test_data(num_records=1000):
    """Generate specified number of NER test data records"""
    data = []

    for _ in range(num_records):
        # Randomly select two different persons
        person1, person2 = random.sample(persons, 2)

        # Randomly select two different organizations
        org1, org2 = random.sample(organizations, 2)

        # Randomly select one location
        loc = random.choice(locations)

        # Randomly select a template
        template = random.choice(templates)

        # Fill in the template
        sentence = template.format(
            person1=person1, person2=person2, org1=org1, org2=org2, loc=loc
        )

        # Each record has the format: data, persons, org, loc
        record = [sentence, 2, 2, 1]
        data.append(record)

    return data


def save_data(data, output_file):
    """Save generated data to CSV file"""
    with open(output_file, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerows(data)
    print(f"Added {len(data)} records to {output_file}")


if __name__ == "__main__":
    # File path for NER test data
    output_file = os.path.join("data", "ner_test_data.csv")

    # Generate 1000 additional records
    additional_records = generate_ner_test_data(1000)

    # Save data to CSV file
    save_data(additional_records, output_file)

    print(f"Generated data at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
