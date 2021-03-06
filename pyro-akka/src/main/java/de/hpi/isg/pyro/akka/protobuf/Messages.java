// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: pyro-akka/src/main/protobuf/messages.proto

package de.hpi.isg.pyro.akka.protobuf;

public final class Messages {
  private Messages() {}
  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistry registry) {
  }
  public interface DependencyMsgOrBuilder
      extends com.google.protobuf.MessageOrBuilder {

    // required .messages.DependencyMsg.DependencyType dependencyType = 1;
    /**
     * <code>required .messages.DependencyMsg.DependencyType dependencyType = 1;</code>
     */
    boolean hasDependencyType();
    /**
     * <code>required .messages.DependencyMsg.DependencyType dependencyType = 1;</code>
     */
    de.hpi.isg.pyro.akka.protobuf.Messages.DependencyMsg.DependencyType getDependencyType();

    // required double error = 2;
    /**
     * <code>required double error = 2;</code>
     */
    boolean hasError();
    /**
     * <code>required double error = 2;</code>
     */
    double getError();

    // required double score = 3;
    /**
     * <code>required double score = 3;</code>
     */
    boolean hasScore();
    /**
     * <code>required double score = 3;</code>
     */
    double getScore();

    // repeated int32 lhs = 4;
    /**
     * <code>repeated int32 lhs = 4;</code>
     */
    java.util.List<java.lang.Integer> getLhsList();
    /**
     * <code>repeated int32 lhs = 4;</code>
     */
    int getLhsCount();
    /**
     * <code>repeated int32 lhs = 4;</code>
     */
    int getLhs(int index);

    // optional int32 rhs = 5;
    /**
     * <code>optional int32 rhs = 5;</code>
     */
    boolean hasRhs();
    /**
     * <code>optional int32 rhs = 5;</code>
     */
    int getRhs();
  }
  /**
   * Protobuf type {@code messages.DependencyMsg}
   */
  public static final class DependencyMsg extends
      com.google.protobuf.GeneratedMessage
      implements DependencyMsgOrBuilder {
    // Use DependencyMsg.newBuilder() to construct.
    private DependencyMsg(com.google.protobuf.GeneratedMessage.Builder<?> builder) {
      super(builder);
      this.unknownFields = builder.getUnknownFields();
    }
    private DependencyMsg(boolean noInit) { this.unknownFields = com.google.protobuf.UnknownFieldSet.getDefaultInstance(); }

    private static final DependencyMsg defaultInstance;
    public static DependencyMsg getDefaultInstance() {
      return defaultInstance;
    }

    public DependencyMsg getDefaultInstanceForType() {
      return defaultInstance;
    }

    private final com.google.protobuf.UnknownFieldSet unknownFields;
    @java.lang.Override
    public final com.google.protobuf.UnknownFieldSet
        getUnknownFields() {
      return this.unknownFields;
    }
    private DependencyMsg(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      initFields();
      int mutable_bitField0_ = 0;
      com.google.protobuf.UnknownFieldSet.Builder unknownFields =
          com.google.protobuf.UnknownFieldSet.newBuilder();
      try {
        boolean done = false;
        while (!done) {
          int tag = input.readTag();
          switch (tag) {
            case 0:
              done = true;
              break;
            default: {
              if (!parseUnknownField(input, unknownFields,
                                     extensionRegistry, tag)) {
                done = true;
              }
              break;
            }
            case 8: {
              int rawValue = input.readEnum();
              de.hpi.isg.pyro.akka.protobuf.Messages.DependencyMsg.DependencyType value = de.hpi.isg.pyro.akka.protobuf.Messages.DependencyMsg.DependencyType.valueOf(rawValue);
              if (value == null) {
                unknownFields.mergeVarintField(1, rawValue);
              } else {
                bitField0_ |= 0x00000001;
                dependencyType_ = value;
              }
              break;
            }
            case 17: {
              bitField0_ |= 0x00000002;
              error_ = input.readDouble();
              break;
            }
            case 25: {
              bitField0_ |= 0x00000004;
              score_ = input.readDouble();
              break;
            }
            case 32: {
              if (!((mutable_bitField0_ & 0x00000008) == 0x00000008)) {
                lhs_ = new java.util.ArrayList<java.lang.Integer>();
                mutable_bitField0_ |= 0x00000008;
              }
              lhs_.add(input.readInt32());
              break;
            }
            case 34: {
              int length = input.readRawVarint32();
              int limit = input.pushLimit(length);
              if (!((mutable_bitField0_ & 0x00000008) == 0x00000008) && input.getBytesUntilLimit() > 0) {
                lhs_ = new java.util.ArrayList<java.lang.Integer>();
                mutable_bitField0_ |= 0x00000008;
              }
              while (input.getBytesUntilLimit() > 0) {
                lhs_.add(input.readInt32());
              }
              input.popLimit(limit);
              break;
            }
            case 40: {
              bitField0_ |= 0x00000008;
              rhs_ = input.readInt32();
              break;
            }
          }
        }
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        throw e.setUnfinishedMessage(this);
      } catch (java.io.IOException e) {
        throw new com.google.protobuf.InvalidProtocolBufferException(
            e.getMessage()).setUnfinishedMessage(this);
      } finally {
        if (((mutable_bitField0_ & 0x00000008) == 0x00000008)) {
          lhs_ = java.util.Collections.unmodifiableList(lhs_);
        }
        this.unknownFields = unknownFields.build();
        makeExtensionsImmutable();
      }
    }
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return de.hpi.isg.pyro.akka.protobuf.Messages.internal_static_messages_DependencyMsg_descriptor;
    }

    protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return de.hpi.isg.pyro.akka.protobuf.Messages.internal_static_messages_DependencyMsg_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              de.hpi.isg.pyro.akka.protobuf.Messages.DependencyMsg.class, de.hpi.isg.pyro.akka.protobuf.Messages.DependencyMsg.Builder.class);
    }

    public static com.google.protobuf.Parser<DependencyMsg> PARSER =
        new com.google.protobuf.AbstractParser<DependencyMsg>() {
      public DependencyMsg parsePartialFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws com.google.protobuf.InvalidProtocolBufferException {
        return new DependencyMsg(input, extensionRegistry);
      }
    };

    @java.lang.Override
    public com.google.protobuf.Parser<DependencyMsg> getParserForType() {
      return PARSER;
    }

    /**
     * Protobuf enum {@code messages.DependencyMsg.DependencyType}
     */
    public enum DependencyType
        implements com.google.protobuf.ProtocolMessageEnum {
      /**
       * <code>FD = 0;</code>
       */
      FD(0, 0),
      /**
       * <code>UCC = 1;</code>
       */
      UCC(1, 1),
      ;

      /**
       * <code>FD = 0;</code>
       */
      public static final int FD_VALUE = 0;
      /**
       * <code>UCC = 1;</code>
       */
      public static final int UCC_VALUE = 1;


      public final int getNumber() { return value; }

      public static DependencyType valueOf(int value) {
        switch (value) {
          case 0: return FD;
          case 1: return UCC;
          default: return null;
        }
      }

      public static com.google.protobuf.Internal.EnumLiteMap<DependencyType>
          internalGetValueMap() {
        return internalValueMap;
      }
      private static com.google.protobuf.Internal.EnumLiteMap<DependencyType>
          internalValueMap =
            new com.google.protobuf.Internal.EnumLiteMap<DependencyType>() {
              public DependencyType findValueByNumber(int number) {
                return DependencyType.valueOf(number);
              }
            };

      public final com.google.protobuf.Descriptors.EnumValueDescriptor
          getValueDescriptor() {
        return getDescriptor().getValues().get(index);
      }
      public final com.google.protobuf.Descriptors.EnumDescriptor
          getDescriptorForType() {
        return getDescriptor();
      }
      public static final com.google.protobuf.Descriptors.EnumDescriptor
          getDescriptor() {
        return de.hpi.isg.pyro.akka.protobuf.Messages.DependencyMsg.getDescriptor().getEnumTypes().get(0);
      }

      private static final DependencyType[] VALUES = values();

      public static DependencyType valueOf(
          com.google.protobuf.Descriptors.EnumValueDescriptor desc) {
        if (desc.getType() != getDescriptor()) {
          throw new java.lang.IllegalArgumentException(
            "EnumValueDescriptor is not for this type.");
        }
        return VALUES[desc.getIndex()];
      }

      private final int index;
      private final int value;

      private DependencyType(int index, int value) {
        this.index = index;
        this.value = value;
      }

      // @@protoc_insertion_point(enum_scope:messages.DependencyMsg.DependencyType)
    }

    private int bitField0_;
    // required .messages.DependencyMsg.DependencyType dependencyType = 1;
    public static final int DEPENDENCYTYPE_FIELD_NUMBER = 1;
    private de.hpi.isg.pyro.akka.protobuf.Messages.DependencyMsg.DependencyType dependencyType_;
    /**
     * <code>required .messages.DependencyMsg.DependencyType dependencyType = 1;</code>
     */
    public boolean hasDependencyType() {
      return ((bitField0_ & 0x00000001) == 0x00000001);
    }
    /**
     * <code>required .messages.DependencyMsg.DependencyType dependencyType = 1;</code>
     */
    public de.hpi.isg.pyro.akka.protobuf.Messages.DependencyMsg.DependencyType getDependencyType() {
      return dependencyType_;
    }

    // required double error = 2;
    public static final int ERROR_FIELD_NUMBER = 2;
    private double error_;
    /**
     * <code>required double error = 2;</code>
     */
    public boolean hasError() {
      return ((bitField0_ & 0x00000002) == 0x00000002);
    }
    /**
     * <code>required double error = 2;</code>
     */
    public double getError() {
      return error_;
    }

    // required double score = 3;
    public static final int SCORE_FIELD_NUMBER = 3;
    private double score_;
    /**
     * <code>required double score = 3;</code>
     */
    public boolean hasScore() {
      return ((bitField0_ & 0x00000004) == 0x00000004);
    }
    /**
     * <code>required double score = 3;</code>
     */
    public double getScore() {
      return score_;
    }

    // repeated int32 lhs = 4;
    public static final int LHS_FIELD_NUMBER = 4;
    private java.util.List<java.lang.Integer> lhs_;
    /**
     * <code>repeated int32 lhs = 4;</code>
     */
    public java.util.List<java.lang.Integer>
        getLhsList() {
      return lhs_;
    }
    /**
     * <code>repeated int32 lhs = 4;</code>
     */
    public int getLhsCount() {
      return lhs_.size();
    }
    /**
     * <code>repeated int32 lhs = 4;</code>
     */
    public int getLhs(int index) {
      return lhs_.get(index);
    }

    // optional int32 rhs = 5;
    public static final int RHS_FIELD_NUMBER = 5;
    private int rhs_;
    /**
     * <code>optional int32 rhs = 5;</code>
     */
    public boolean hasRhs() {
      return ((bitField0_ & 0x00000008) == 0x00000008);
    }
    /**
     * <code>optional int32 rhs = 5;</code>
     */
    public int getRhs() {
      return rhs_;
    }

    private void initFields() {
      dependencyType_ = de.hpi.isg.pyro.akka.protobuf.Messages.DependencyMsg.DependencyType.FD;
      error_ = 0D;
      score_ = 0D;
      lhs_ = java.util.Collections.emptyList();
      rhs_ = 0;
    }
    private byte memoizedIsInitialized = -1;
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized != -1) return isInitialized == 1;

      if (!hasDependencyType()) {
        memoizedIsInitialized = 0;
        return false;
      }
      if (!hasError()) {
        memoizedIsInitialized = 0;
        return false;
      }
      if (!hasScore()) {
        memoizedIsInitialized = 0;
        return false;
      }
      memoizedIsInitialized = 1;
      return true;
    }

    public void writeTo(com.google.protobuf.CodedOutputStream output)
                        throws java.io.IOException {
      getSerializedSize();
      if (((bitField0_ & 0x00000001) == 0x00000001)) {
        output.writeEnum(1, dependencyType_.getNumber());
      }
      if (((bitField0_ & 0x00000002) == 0x00000002)) {
        output.writeDouble(2, error_);
      }
      if (((bitField0_ & 0x00000004) == 0x00000004)) {
        output.writeDouble(3, score_);
      }
      for (int i = 0; i < lhs_.size(); i++) {
        output.writeInt32(4, lhs_.get(i));
      }
      if (((bitField0_ & 0x00000008) == 0x00000008)) {
        output.writeInt32(5, rhs_);
      }
      getUnknownFields().writeTo(output);
    }

    private int memoizedSerializedSize = -1;
    public int getSerializedSize() {
      int size = memoizedSerializedSize;
      if (size != -1) return size;

      size = 0;
      if (((bitField0_ & 0x00000001) == 0x00000001)) {
        size += com.google.protobuf.CodedOutputStream
          .computeEnumSize(1, dependencyType_.getNumber());
      }
      if (((bitField0_ & 0x00000002) == 0x00000002)) {
        size += com.google.protobuf.CodedOutputStream
          .computeDoubleSize(2, error_);
      }
      if (((bitField0_ & 0x00000004) == 0x00000004)) {
        size += com.google.protobuf.CodedOutputStream
          .computeDoubleSize(3, score_);
      }
      {
        int dataSize = 0;
        for (int i = 0; i < lhs_.size(); i++) {
          dataSize += com.google.protobuf.CodedOutputStream
            .computeInt32SizeNoTag(lhs_.get(i));
        }
        size += dataSize;
        size += 1 * getLhsList().size();
      }
      if (((bitField0_ & 0x00000008) == 0x00000008)) {
        size += com.google.protobuf.CodedOutputStream
          .computeInt32Size(5, rhs_);
      }
      size += getUnknownFields().getSerializedSize();
      memoizedSerializedSize = size;
      return size;
    }

    private static final long serialVersionUID = 0L;
    @java.lang.Override
    protected java.lang.Object writeReplace()
        throws java.io.ObjectStreamException {
      return super.writeReplace();
    }

    public static de.hpi.isg.pyro.akka.protobuf.Messages.DependencyMsg parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static de.hpi.isg.pyro.akka.protobuf.Messages.DependencyMsg parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static de.hpi.isg.pyro.akka.protobuf.Messages.DependencyMsg parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static de.hpi.isg.pyro.akka.protobuf.Messages.DependencyMsg parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static de.hpi.isg.pyro.akka.protobuf.Messages.DependencyMsg parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return PARSER.parseFrom(input);
    }
    public static de.hpi.isg.pyro.akka.protobuf.Messages.DependencyMsg parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }
    public static de.hpi.isg.pyro.akka.protobuf.Messages.DependencyMsg parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input);
    }
    public static de.hpi.isg.pyro.akka.protobuf.Messages.DependencyMsg parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input, extensionRegistry);
    }
    public static de.hpi.isg.pyro.akka.protobuf.Messages.DependencyMsg parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return PARSER.parseFrom(input);
    }
    public static de.hpi.isg.pyro.akka.protobuf.Messages.DependencyMsg parseFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }

    public static Builder newBuilder() { return Builder.create(); }
    public Builder newBuilderForType() { return newBuilder(); }
    public static Builder newBuilder(de.hpi.isg.pyro.akka.protobuf.Messages.DependencyMsg prototype) {
      return newBuilder().mergeFrom(prototype);
    }
    public Builder toBuilder() { return newBuilder(this); }

    @java.lang.Override
    protected Builder newBuilderForType(
        com.google.protobuf.GeneratedMessage.BuilderParent parent) {
      Builder builder = new Builder(parent);
      return builder;
    }
    /**
     * Protobuf type {@code messages.DependencyMsg}
     */
    public static final class Builder extends
        com.google.protobuf.GeneratedMessage.Builder<Builder>
       implements de.hpi.isg.pyro.akka.protobuf.Messages.DependencyMsgOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor
          getDescriptor() {
        return de.hpi.isg.pyro.akka.protobuf.Messages.internal_static_messages_DependencyMsg_descriptor;
      }

      protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
          internalGetFieldAccessorTable() {
        return de.hpi.isg.pyro.akka.protobuf.Messages.internal_static_messages_DependencyMsg_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                de.hpi.isg.pyro.akka.protobuf.Messages.DependencyMsg.class, de.hpi.isg.pyro.akka.protobuf.Messages.DependencyMsg.Builder.class);
      }

      // Construct using de.hpi.isg.pyro.akka.protobuf.Messages.DependencyMsg.newBuilder()
      private Builder() {
        maybeForceBuilderInitialization();
      }

      private Builder(
          com.google.protobuf.GeneratedMessage.BuilderParent parent) {
        super(parent);
        maybeForceBuilderInitialization();
      }
      private void maybeForceBuilderInitialization() {
        if (com.google.protobuf.GeneratedMessage.alwaysUseFieldBuilders) {
        }
      }
      private static Builder create() {
        return new Builder();
      }

      public Builder clear() {
        super.clear();
        dependencyType_ = de.hpi.isg.pyro.akka.protobuf.Messages.DependencyMsg.DependencyType.FD;
        bitField0_ = (bitField0_ & ~0x00000001);
        error_ = 0D;
        bitField0_ = (bitField0_ & ~0x00000002);
        score_ = 0D;
        bitField0_ = (bitField0_ & ~0x00000004);
        lhs_ = java.util.Collections.emptyList();
        bitField0_ = (bitField0_ & ~0x00000008);
        rhs_ = 0;
        bitField0_ = (bitField0_ & ~0x00000010);
        return this;
      }

      public Builder clone() {
        return create().mergeFrom(buildPartial());
      }

      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return de.hpi.isg.pyro.akka.protobuf.Messages.internal_static_messages_DependencyMsg_descriptor;
      }

      public de.hpi.isg.pyro.akka.protobuf.Messages.DependencyMsg getDefaultInstanceForType() {
        return de.hpi.isg.pyro.akka.protobuf.Messages.DependencyMsg.getDefaultInstance();
      }

      public de.hpi.isg.pyro.akka.protobuf.Messages.DependencyMsg build() {
        de.hpi.isg.pyro.akka.protobuf.Messages.DependencyMsg result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      public de.hpi.isg.pyro.akka.protobuf.Messages.DependencyMsg buildPartial() {
        de.hpi.isg.pyro.akka.protobuf.Messages.DependencyMsg result = new de.hpi.isg.pyro.akka.protobuf.Messages.DependencyMsg(this);
        int from_bitField0_ = bitField0_;
        int to_bitField0_ = 0;
        if (((from_bitField0_ & 0x00000001) == 0x00000001)) {
          to_bitField0_ |= 0x00000001;
        }
        result.dependencyType_ = dependencyType_;
        if (((from_bitField0_ & 0x00000002) == 0x00000002)) {
          to_bitField0_ |= 0x00000002;
        }
        result.error_ = error_;
        if (((from_bitField0_ & 0x00000004) == 0x00000004)) {
          to_bitField0_ |= 0x00000004;
        }
        result.score_ = score_;
        if (((bitField0_ & 0x00000008) == 0x00000008)) {
          lhs_ = java.util.Collections.unmodifiableList(lhs_);
          bitField0_ = (bitField0_ & ~0x00000008);
        }
        result.lhs_ = lhs_;
        if (((from_bitField0_ & 0x00000010) == 0x00000010)) {
          to_bitField0_ |= 0x00000008;
        }
        result.rhs_ = rhs_;
        result.bitField0_ = to_bitField0_;
        onBuilt();
        return result;
      }

      public Builder mergeFrom(com.google.protobuf.Message other) {
        if (other instanceof de.hpi.isg.pyro.akka.protobuf.Messages.DependencyMsg) {
          return mergeFrom((de.hpi.isg.pyro.akka.protobuf.Messages.DependencyMsg)other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(de.hpi.isg.pyro.akka.protobuf.Messages.DependencyMsg other) {
        if (other == de.hpi.isg.pyro.akka.protobuf.Messages.DependencyMsg.getDefaultInstance()) return this;
        if (other.hasDependencyType()) {
          setDependencyType(other.getDependencyType());
        }
        if (other.hasError()) {
          setError(other.getError());
        }
        if (other.hasScore()) {
          setScore(other.getScore());
        }
        if (!other.lhs_.isEmpty()) {
          if (lhs_.isEmpty()) {
            lhs_ = other.lhs_;
            bitField0_ = (bitField0_ & ~0x00000008);
          } else {
            ensureLhsIsMutable();
            lhs_.addAll(other.lhs_);
          }
          onChanged();
        }
        if (other.hasRhs()) {
          setRhs(other.getRhs());
        }
        this.mergeUnknownFields(other.getUnknownFields());
        return this;
      }

      public final boolean isInitialized() {
        if (!hasDependencyType()) {
          
          return false;
        }
        if (!hasError()) {
          
          return false;
        }
        if (!hasScore()) {
          
          return false;
        }
        return true;
      }

      public Builder mergeFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        de.hpi.isg.pyro.akka.protobuf.Messages.DependencyMsg parsedMessage = null;
        try {
          parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          parsedMessage = (de.hpi.isg.pyro.akka.protobuf.Messages.DependencyMsg) e.getUnfinishedMessage();
          throw e;
        } finally {
          if (parsedMessage != null) {
            mergeFrom(parsedMessage);
          }
        }
        return this;
      }
      private int bitField0_;

      // required .messages.DependencyMsg.DependencyType dependencyType = 1;
      private de.hpi.isg.pyro.akka.protobuf.Messages.DependencyMsg.DependencyType dependencyType_ = de.hpi.isg.pyro.akka.protobuf.Messages.DependencyMsg.DependencyType.FD;
      /**
       * <code>required .messages.DependencyMsg.DependencyType dependencyType = 1;</code>
       */
      public boolean hasDependencyType() {
        return ((bitField0_ & 0x00000001) == 0x00000001);
      }
      /**
       * <code>required .messages.DependencyMsg.DependencyType dependencyType = 1;</code>
       */
      public de.hpi.isg.pyro.akka.protobuf.Messages.DependencyMsg.DependencyType getDependencyType() {
        return dependencyType_;
      }
      /**
       * <code>required .messages.DependencyMsg.DependencyType dependencyType = 1;</code>
       */
      public Builder setDependencyType(de.hpi.isg.pyro.akka.protobuf.Messages.DependencyMsg.DependencyType value) {
        if (value == null) {
          throw new NullPointerException();
        }
        bitField0_ |= 0x00000001;
        dependencyType_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>required .messages.DependencyMsg.DependencyType dependencyType = 1;</code>
       */
      public Builder clearDependencyType() {
        bitField0_ = (bitField0_ & ~0x00000001);
        dependencyType_ = de.hpi.isg.pyro.akka.protobuf.Messages.DependencyMsg.DependencyType.FD;
        onChanged();
        return this;
      }

      // required double error = 2;
      private double error_ ;
      /**
       * <code>required double error = 2;</code>
       */
      public boolean hasError() {
        return ((bitField0_ & 0x00000002) == 0x00000002);
      }
      /**
       * <code>required double error = 2;</code>
       */
      public double getError() {
        return error_;
      }
      /**
       * <code>required double error = 2;</code>
       */
      public Builder setError(double value) {
        bitField0_ |= 0x00000002;
        error_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>required double error = 2;</code>
       */
      public Builder clearError() {
        bitField0_ = (bitField0_ & ~0x00000002);
        error_ = 0D;
        onChanged();
        return this;
      }

      // required double score = 3;
      private double score_ ;
      /**
       * <code>required double score = 3;</code>
       */
      public boolean hasScore() {
        return ((bitField0_ & 0x00000004) == 0x00000004);
      }
      /**
       * <code>required double score = 3;</code>
       */
      public double getScore() {
        return score_;
      }
      /**
       * <code>required double score = 3;</code>
       */
      public Builder setScore(double value) {
        bitField0_ |= 0x00000004;
        score_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>required double score = 3;</code>
       */
      public Builder clearScore() {
        bitField0_ = (bitField0_ & ~0x00000004);
        score_ = 0D;
        onChanged();
        return this;
      }

      // repeated int32 lhs = 4;
      private java.util.List<java.lang.Integer> lhs_ = java.util.Collections.emptyList();
      private void ensureLhsIsMutable() {
        if (!((bitField0_ & 0x00000008) == 0x00000008)) {
          lhs_ = new java.util.ArrayList<java.lang.Integer>(lhs_);
          bitField0_ |= 0x00000008;
         }
      }
      /**
       * <code>repeated int32 lhs = 4;</code>
       */
      public java.util.List<java.lang.Integer>
          getLhsList() {
        return java.util.Collections.unmodifiableList(lhs_);
      }
      /**
       * <code>repeated int32 lhs = 4;</code>
       */
      public int getLhsCount() {
        return lhs_.size();
      }
      /**
       * <code>repeated int32 lhs = 4;</code>
       */
      public int getLhs(int index) {
        return lhs_.get(index);
      }
      /**
       * <code>repeated int32 lhs = 4;</code>
       */
      public Builder setLhs(
          int index, int value) {
        ensureLhsIsMutable();
        lhs_.set(index, value);
        onChanged();
        return this;
      }
      /**
       * <code>repeated int32 lhs = 4;</code>
       */
      public Builder addLhs(int value) {
        ensureLhsIsMutable();
        lhs_.add(value);
        onChanged();
        return this;
      }
      /**
       * <code>repeated int32 lhs = 4;</code>
       */
      public Builder addAllLhs(
          java.lang.Iterable<? extends java.lang.Integer> values) {
        ensureLhsIsMutable();
        super.addAll(values, lhs_);
        onChanged();
        return this;
      }
      /**
       * <code>repeated int32 lhs = 4;</code>
       */
      public Builder clearLhs() {
        lhs_ = java.util.Collections.emptyList();
        bitField0_ = (bitField0_ & ~0x00000008);
        onChanged();
        return this;
      }

      // optional int32 rhs = 5;
      private int rhs_ ;
      /**
       * <code>optional int32 rhs = 5;</code>
       */
      public boolean hasRhs() {
        return ((bitField0_ & 0x00000010) == 0x00000010);
      }
      /**
       * <code>optional int32 rhs = 5;</code>
       */
      public int getRhs() {
        return rhs_;
      }
      /**
       * <code>optional int32 rhs = 5;</code>
       */
      public Builder setRhs(int value) {
        bitField0_ |= 0x00000010;
        rhs_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional int32 rhs = 5;</code>
       */
      public Builder clearRhs() {
        bitField0_ = (bitField0_ & ~0x00000010);
        rhs_ = 0;
        onChanged();
        return this;
      }

      // @@protoc_insertion_point(builder_scope:messages.DependencyMsg)
    }

    static {
      defaultInstance = new DependencyMsg(true);
      defaultInstance.initFields();
    }

    // @@protoc_insertion_point(class_scope:messages.DependencyMsg)
  }

  private static com.google.protobuf.Descriptors.Descriptor
    internal_static_messages_DependencyMsg_descriptor;
  private static
    com.google.protobuf.GeneratedMessage.FieldAccessorTable
      internal_static_messages_DependencyMsg_fieldAccessorTable;

  public static com.google.protobuf.Descriptors.FileDescriptor
      getDescriptor() {
    return descriptor;
  }
  private static com.google.protobuf.Descriptors.FileDescriptor
      descriptor;
  static {
    java.lang.String[] descriptorData = {
      "\n*pyro-akka/src/main/protobuf/messages.p" +
      "roto\022\010messages\"\252\001\n\rDependencyMsg\022>\n\016depe" +
      "ndencyType\030\001 \002(\0162&.messages.DependencyMs" +
      "g.DependencyType\022\r\n\005error\030\002 \002(\001\022\r\n\005score" +
      "\030\003 \002(\001\022\013\n\003lhs\030\004 \003(\005\022\013\n\003rhs\030\005 \001(\005\"!\n\016Depe" +
      "ndencyType\022\006\n\002FD\020\000\022\007\n\003UCC\020\001B\037\n\035de.hpi.is" +
      "g.pyro.akka.protobuf"
    };
    com.google.protobuf.Descriptors.FileDescriptor.InternalDescriptorAssigner assigner =
      new com.google.protobuf.Descriptors.FileDescriptor.InternalDescriptorAssigner() {
        public com.google.protobuf.ExtensionRegistry assignDescriptors(
            com.google.protobuf.Descriptors.FileDescriptor root) {
          descriptor = root;
          internal_static_messages_DependencyMsg_descriptor =
            getDescriptor().getMessageTypes().get(0);
          internal_static_messages_DependencyMsg_fieldAccessorTable = new
            com.google.protobuf.GeneratedMessage.FieldAccessorTable(
              internal_static_messages_DependencyMsg_descriptor,
              new java.lang.String[] { "DependencyType", "Error", "Score", "Lhs", "Rhs", });
          return null;
        }
      };
    com.google.protobuf.Descriptors.FileDescriptor
      .internalBuildGeneratedFileFrom(descriptorData,
        new com.google.protobuf.Descriptors.FileDescriptor[] {
        }, assigner);
  }

  // @@protoc_insertion_point(outer_class_scope)
}
