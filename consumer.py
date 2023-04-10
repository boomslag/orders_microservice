import json, os, django
from confluent_kafka import Consumer
import uuid
import requests
from decimal import Decimal

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "core.settings")
django.setup()
from django.db import transaction
from rest_framework.exceptions import ValidationError

from django.apps import apps
from core.producer import producer

Order = apps.get_model('orders', 'Order')
OrderItem = apps.get_model('orders', 'OrderItem')

consumer1 = Consumer({
    'bootstrap.servers': os.environ.get('KAFKA_BOOTSTRAP_SERVER'),
    'security.protocol': os.environ.get('KAFKA_SECURITY_PROTOCOL'),
    'sasl.username': os.environ.get('KAFKA_USERNAME'),
    'sasl.password': os.environ.get('KAFKA_PASSWORD'),
    'sasl.mechanism': 'PLAIN',
    'group.id': os.environ.get('KAFKA_GROUP'),
    'auto.offset.reset': 'earliest'
})
consumer1.subscribe([os.environ.get('KAFKA_TOPIC')])

def create_order(order_data):
    cart_items = order_data['cartItems']
    delivery_address = order_data['deliveryAddress']
    save_delivery_address = order_data['saveDeliveryAddress']
    user_id = order_data['userID']

    courses = []
    products = []

    # Create the order and order items inside a transaction
    with transaction.atomic():
        order = Order.objects.create(
            user=user_id,
            transaction_id=str(uuid.uuid4())
        )
        for item in cart_items:

            if item.get('course'):
                courses.append(item)
            elif item.get('product'):
                products.append(item)

        for object in courses:
            course_id = object['course'] if object['course'] else None
            course_response = requests.get('http://host.docker.internal:8004/api/courses/get/' + object['course'] + '/').json()
            if not course_id:
                raise ValidationError('No course ID provided for item.')
            order_item = OrderItem.objects.create(
                course=course_id,
                order=order,
                name=course_response.get('results').get('details').get('title'),
                price=course_response.get('results').get('details').get('price'),
                count=item.get('count') or 1
            )
            order.amount+= Decimal(course_response.get('results').get('details').get('price'))
            order.order_items.add(order_item)
            order.save()
        
        for object in products:
            product_id = object['product'] if object['product'] else None
            if not product_id:
                raise ValidationError('No product ID provided for item.')
            shipping_id = object['shipping'] if object['shipping'] else None
            if not shipping_id:
                raise ValidationError('No Shipping ID provided for item.')
            
            # Fetch product details from product API endpoint
            product_response = requests.get(f'http://host.docker.internal:8003/api/products/get/{product_id}/').json()

            # GET Shipping
            shipping_id = object['shipping']
            filtered_shipping = [shipping for shipping in product_response.get('results').get('shipping') if shipping['id'] == shipping_id]
            selected_shipping = None
            if filtered_shipping:
                selected_shipping = filtered_shipping[0]
           
            # GET WEIGHT
            weight_id = object['weight']
            filtered_weights = [weight for weight in product_response.get('results').get('weights') if weight['id'] == weight_id]
            selected_weight = None
            if filtered_weights:
                selected_weight = filtered_weights[0]
                # kafka producer to reduce stock
                producer_data = {
                    'product_id': product_id, 
                    'weight_id': weight_id, 
                    'stock_to_reduce': object['count']
                }
                producer.produce(
                    'product_purchased', 
                    key=b'reduce_stock_by_weight', 
                    value=json.dumps(producer_data).encode('utf-8')
                )
                producer.flush()

            # GET MATERIAL
            material_id = object['material']
            filtered_materials = [material for material in product_response.get('results').get('materials') if material['id'] == material_id]
            selected_material = None
            if filtered_materials:
                selected_material = filtered_materials[0]
                # kafka producer to reduce stock
                producer_data = {
                    'product_id': product_id, 
                    'material_id': material_id, 
                    'stock_to_reduce': object['count']
                }
                producer.produce(
                    'product_purchased', 
                    key=b'reduce_stock_by_material', 
                    value=json.dumps(producer_data).encode('utf-8')
                )
                producer.flush()

            # GET COLOR
            color_id = object['color']
            filtered_colors = [color for color in product_response.get('results').get('colors') if color['id'] == color_id]
            selected_color = None
            if filtered_colors:
                selected_color = filtered_colors[0]
                # kafka producer to reduce stock
                producer_data = {
                    'product_id': product_id, 
                    'color_id': color_id, 
                    'stock_to_reduce': object['count']
                }
                producer.produce(
                    'product_purchased', 
                    key=b'reduce_stock_by_color', 
                    value=json.dumps(producer_data).encode('utf-8')
                )
                producer.flush()

            # GET Size
            size_id = object['size']
            filtered_sizes = [size for size in product_response.get('results').get('sizes') if size['id'] == size_id]
            selected_size = None
            if filtered_sizes:
                selected_size = filtered_sizes[0]
                # kafka producer to reduce stock
                producer_data = {
                    'product_id': product_id, 
                        'size_id': size_id, 
                        'stock_to_reduce': object['count']
                }
                producer.produce(
                    'product_purchased', 
                    key=b'reduce_stock_by_size', 
                    value=json.dumps(producer_data).encode('utf-8')
                )
                producer.flush()

            base_price= Decimal('0')
            product_price = product_response.get('results').get('details').get('price')
            if product_price is not None:
                base_price = product_price
            else:
                if selected_weight:
                    base_price += Decimal(selected_weight.get('price'))
                if selected_material:
                    base_price += Decimal(selected_material.get('price'))
                if selected_color:
                    base_price += Decimal(selected_color.get('price'))
                if selected_size:
                    base_price += Decimal(selected_size.get('price'))

            # Create OrderItem instance for the product
            try:
                first_image_file = product_response.get('results').get('images')[0].get('file') if product_response.get('results').get('images') else None
                order_item = OrderItem.objects.create(
                    product=product_response.get('results').get('details').get('id'),
                    thumbnail=first_image_file,                order=order,
                    name=product_response.get('results').get('details').get('title'),
                    price=base_price,
                    count=object['count'] or 1,
                    
                    color=object['color'],
                    color_hex=selected_color.get('hex') if selected_color else None,
                    color_price=selected_color.get('price') if selected_color else None,

                    size=object['size'],
                    size_name=selected_size.get('title') if selected_size else None,
                    size_price=selected_size.get('price') if selected_size else None,

                    weight=object['weight'],
                    weight_name=selected_weight.get('title') if selected_weight else None,
                    weight_price=selected_weight.get('price') if selected_weight else None,

                    material=object['material'],
                    material_name=selected_material.get('title') if selected_material else None,
                    material_price=selected_material.get('price') if selected_material else None,

                    shipping=object['shipping'],
                    shipping_time=selected_shipping.get('time'),
                    shipping_name=selected_shipping.get('title'),
                    shipping_price=selected_shipping.get('price'),

                    buyer=user_id,
                    vendor=product_response.get('results').get('details').get('author'),

                    address_line_1=delivery_address['address_line_1'] ,
                    address_line_2=delivery_address['address_line_2'] if delivery_address['address_line_2'] else None,
                    city=delivery_address['city'],
                    state_province_region=delivery_address['state_province_region'],
                    postal_zip_code=delivery_address['postal_zip_code'],
                    country_region=delivery_address['country_region'],
                    telephone_number=delivery_address['telephone_number'] if delivery_address['telephone_number'] else None,
                    # Add any additional fields as needed
                )

                # Add the OrderItem instance to the order
                order.amount += Decimal(base_price)
                order.order_items.add(order_item)
                order.save()
                print(f"Order {order_item} created")
            except:
                print('Failed to create order item')
                continue

            # IF AGreed, Send information to Kafka Consumer Auth
            if save_delivery_address == True:
                delivery_address_data = {
                    'user_id': user_id,
                    'delivery_address': delivery_address
                }
                producer.produce(
                    'delivery_address',
                    key='store_delivery_address',
                    value=json.dumps(delivery_address_data).encode('utf-8')
                )
                producer.flush()

while True:
    msg1 = consumer1.poll(1.0)

    if msg1 is None:
        continue
    if msg1.error():
        print("Consumer error: {}".format(msg1.error()))
        continue

    if msg1 is not None and not msg1.error():
        topic1 = msg1.topic()
        value1 = msg1.value()
        order_data = json.loads(value1)

        try:
            create_order(order_data)
            print(f"Order created successfully for user {order_data['userID']}")
        except ValidationError as e:
            print(f"Failed to create order for user {order_data['userID']}: {str(e)}")

consumer1.close()