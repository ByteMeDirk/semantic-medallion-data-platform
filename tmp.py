import csv
import random
from typing import Dict, List, Tuple

import pandas as pd


def create_test_sentences(
    entity_data: List[Dict], num_records: int = 1000
) -> List[Dict]:
    """
    Generate test sentences with known entity counts for NER validation
    """

    # Expanded template sentences for more variety
    sentence_templates = [
        "{person1} and {person2} discussed partnerships between {org1} and {org2} at their {location} headquarters.",
        "{person1}, CEO of {org1}, announced a new initiative in {location} alongside {person2} from {org2}.",
        "The collaboration between {person1} of {org1} and {person2} of {org2} will transform operations in {location}.",
        "{person1} met with {person2} to discuss {org1}'s expansion into {location} markets.",
        "At the {location} conference, {person1} and {person2} represented {org1} and {org2} respectively.",
        "{person1} from {org1} will be speaking at the {location} summit with {person2} of {org2}.",
        "The partnership between {org1} and {org2}, led by {person1} and {person2}, focuses on {location} markets.",
        "{person1} announced that {org1} will collaborate with {person2}'s {org2} in {location}.",
        "During the {location} meeting, {person1} of {org1} discussed strategies with {person2} from {org2}.",
        "{person1} and {person2} are pioneering new technologies at {org1} and {org2} in {location}.",
        "{person1} of {org1} and {person2} from {org2} signed a major deal in {location}.",
        "The {location} office of {org1} hosted {person1} and {person2} from {org2} for strategic planning.",
        "{person1} announced {org1}'s acquisition of {org2} in a {location} press conference with {person2}.",
        "Investors in {location} are backing the joint venture between {person1}'s {org1} and {person2}'s {org2}.",
        "{person1} and {person2} will co-chair the {location} technology summit representing {org1} and {org2}.",
        "The merger between {org1} and {org2}, championed by {person1} and {person2}, was finalized in {location}.",
        "{person1} from {org1} praised {person2} of {org2} during the {location} innovation awards.",
        "At the {location} headquarters, {person1} of {org1} met with {person2} to discuss {org2}'s future.",
        "{person1} and {person2} announced their companies {org1} and {org2} will open new facilities in {location}.",
        "The {location} tech ecosystem benefits from leaders like {person1} of {org1} and {person2} of {org2}.",
    ]

    # More complex templates with 3+ entities
    complex_templates = [
        "{person1} of {org1}, {person2} from {org2}, and {person3} of {org3} met in {location} to discuss industry trends.",
        "The {location} conference featured {person1} ({org1}), {person2} ({org2}), and {person3} ({org3}) as keynote speakers.",
        "{person1} announced that {org1} will partner with both {org2} and {org3}, with {person2} and {person3} leading the initiative in {location}.",
        "At the {location} summit, {person1} from {org1} joined {person2} of {org2} and {person3} from {org3} for a panel discussion.",
        "The three-way partnership between {org1}, {org2}, and {org3} was celebrated by {person1}, {person2}, and {person3} in {location}.",
    ]

    # Single entity templates
    single_templates = [
        "{person} is revolutionizing the tech industry with innovative approaches.",
        "{org} announced record quarterly earnings exceeding market expectations.",
        "The {location} tech scene is thriving with unprecedented growth this year.",
        "{person} will keynote the upcoming technology conference next month.",
        "{org} is expanding its workforce significantly across multiple departments.",
        "Startups in {location} are attracting major investments from venture capitalists.",
        "{person} received recognition for outstanding leadership in technology innovation.",
        "{org} launched a groundbreaking product that disrupts the market.",
        "The {location} government announced new incentives for tech companies.",
        "{person} published insights on the future of artificial intelligence.",
        "{org} reported strong user growth in the latest quarterly report.",
        "Tech talent is flocking to {location} for better opportunities.",
        "{person} was featured on the cover of a major business magazine.",
        "{org} is investing heavily in research and development initiatives.",
        "The {location} startup ecosystem continues to attract global attention.",
    ]

    # Paragraph templates for longer text
    paragraph_templates = [
        "In a groundbreaking announcement, {person1} of {org1} revealed plans for a strategic partnership with {person2}'s {org2}. The collaboration, set to launch in {location}, aims to revolutionize the industry. {person1} emphasized the importance of innovation, while {person2} highlighted the potential market impact. Both {org1} and {org2} expect significant growth from this {location}-based initiative.",
        "The annual {location} technology summit brought together industry leaders including {person1} from {org1} and {person2} of {org2}. During the event, {person1} announced new developments at {org1}, while {person2} shared insights about {org2}'s future direction. The {location} venue provided an ideal setting for networking and collaboration between the two companies.",
        "Market analysts are closely watching the competition between {org1} and {org2} in the {location} market. {person1}, leading {org1}'s expansion efforts, recently met with local stakeholders, while {person2} of {org2} announced significant investments in the region. Both companies view {location} as crucial for their growth strategies.",
    ]

    # Extract entities from your data
    persons = [
        entity["entity_name"]
        for entity in entity_data
        if entity["entity_type"] == "PERSON"
    ]

    # Enhanced organization extraction
    organizations = set()
    for entity in entity_data:
        description = entity["entity_description"]
        # Extract multiple organizations per person
        orgs_in_desc = []
        if "Microsoft" in description:
            orgs_in_desc.append("Microsoft")
        if "Apple" in description:
            orgs_in_desc.append("Apple")
        if "Tesla" in description:
            orgs_in_desc.append("Tesla")
        if "SpaceX" in description:
            orgs_in_desc.append("SpaceX")
        if "Meta" in description:
            orgs_in_desc.append("Meta")
        if "Facebook" in description:
            orgs_in_desc.append("Facebook")
        if "Google" in description:
            orgs_in_desc.append("Google")
        if "Alphabet" in description:
            orgs_in_desc.append("Alphabet")
        if "Dell" in description:
            orgs_in_desc.append("Dell Technologies")
        if "Amazon" in description:
            orgs_in_desc.append("Amazon")
        if "Netflix" in description:
            orgs_in_desc.append("Netflix")
        if "Twitter" in description:
            orgs_in_desc.append("Twitter")
        if "Oracle" in description:
            orgs_in_desc.append("Oracle")
        if "Nvidia" in description:
            orgs_in_desc.append("Nvidia")
        if "Salesforce" in description:
            orgs_in_desc.append("Salesforce")
        if "Uber" in description:
            orgs_in_desc.append("Uber")
        if "Airbnb" in description:
            orgs_in_desc.append("Airbnb")
        if "Spotify" in description:
            orgs_in_desc.append("Spotify")
        if "Snapchat" in description:
            orgs_in_desc.append("Snapchat")
        if "Dropbox" in description:
            orgs_in_desc.append("Dropbox")
        if "Stripe" in description:
            orgs_in_desc.append("Stripe")
        if "LinkedIn" in description:
            orgs_in_desc.append("LinkedIn")
        if "PayPal" in description:
            orgs_in_desc.append("PayPal")
        if "Palantir" in description:
            orgs_in_desc.append("Palantir")
        if "Asana" in description:
            orgs_in_desc.append("Asana")
        if "Instagram" in description:
            orgs_in_desc.append("Instagram")
        if "Neuralink" in description:
            orgs_in_desc.append("Neuralink")
        if "YouTube" in description:
            orgs_in_desc.append("YouTube")
        if "Square" in description:
            orgs_in_desc.append("Square")

        organizations.update(orgs_in_desc)

    organizations = list(organizations)

    # Expanded locations including international cities
    locations = [
        "Silicon Valley",
        "San Francisco",
        "Seattle",
        "Austin",
        "New York",
        "Boston",
        "Los Angeles",
        "Chicago",
        "Denver",
        "Atlanta",
        "London",
        "Tokyo",
        "Singapore",
        "Toronto",
        "Berlin",
        "Amsterdam",
        "Sydney",
        "Tel Aviv",
        "Bangalore",
        "Dublin",
        "Stockholm",
        "Zurich",
        "Paris",
        "Barcelona",
        "Munich",
    ]

    test_data = []

    # Generate records with different complexity levels
    records_per_type = num_records // 5

    # 1. Simple single-entity sentences (20%)
    for i in range(records_per_type):
        template = random.choice(single_templates)
        entity_type = random.choice(["person", "org", "location"])

        if entity_type == "person" and persons:
            entity = random.choice(persons)
            sentence = template.format(person=entity)
            test_data.append(
                {
                    "data": sentence,
                    "persons": sentence.count(entity),
                    "org": 0,
                    "loc": 0,
                }
            )
        elif entity_type == "org" and organizations:
            entity = random.choice(organizations)
            sentence = template.format(org=entity)
            test_data.append(
                {
                    "data": sentence,
                    "persons": 0,
                    "org": sentence.count(entity),
                    "loc": 0,
                }
            )
        elif entity_type == "location":
            entity = random.choice(locations)
            sentence = template.format(location=entity)
            test_data.append(
                {
                    "data": sentence,
                    "persons": 0,
                    "org": 0,
                    "loc": sentence.count(entity),
                }
            )

    # 2. Two-entity sentences (40%)
    for i in range(records_per_type * 2):
        template = random.choice(sentence_templates)

        person1 = random.choice(persons) if persons else "John Doe"
        person2 = (
            random.choice([p for p in persons if p != person1])
            if len(persons) > 1
            else "Jane Smith"
        )
        org1 = random.choice(organizations) if organizations else "TechCorp"
        org2 = (
            random.choice([o for o in organizations if o != org1])
            if len(organizations) > 1
            else "InnovateInc"
        )
        location = random.choice(locations)

        sentence = template.format(
            person1=person1, person2=person2, org1=org1, org2=org2, location=location
        )

        # Count occurrences
        person_count = sentence.count(person1) + sentence.count(person2)
        org_count = sentence.count(org1) + sentence.count(org2)
        loc_count = sentence.count(location)

        test_data.append(
            {
                "data": sentence,
                "persons": person_count,
                "org": org_count,
                "loc": loc_count,
            }
        )

    # 3. Complex three-entity sentences (20%)
    for i in range(records_per_type):
        template = random.choice(complex_templates)

        selected_persons = random.sample(persons, min(3, len(persons)))
        selected_orgs = random.sample(organizations, min(3, len(organizations)))
